package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
    val logger = LoggerFactory.getLogger("ThetaJoin")
  
    // random samples for each relation
    // helper structures, you are allowed
    // not to use them
    var horizontalBoundaries = Array[Int]()
    var verticalBoundaries = Array[Int]()

    // number of values that fall in each partition
    // helper structures, you are allowed
    // not to use them
    var horizontalCounts = Array[Int]()
    var verticalCounts = Array[Int]()

  /*
   * this method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */  
    def theta_join(dataset1: Dataset, dataset2: Dataset, attr1:String, attr2:String, op:String): RDD[(Int, Int)] = {
        val schema1 = dataset1.getSchema
        val schema2 = dataset2.getSchema

        val index1 = schema1.indexOf(attr1)
        val index2 = schema2.indexOf(attr2)

        val rdd1 = dataset1.getRDD.map(x => x.getInt(index1))
        val rdd2 = dataset2.getRDD.map(x => x.getInt(index2))

        /* COMPUTATION OF EQUI-DEPTH HISTOGRAMS */

        // Calculate number of samples from the two datasets
        val cr = (numR * math.sqrt(reducers.toDouble) / math.sqrt(numR * numS)).toInt
        val cs = (numS * math.sqrt(reducers.toDouble) / math.sqrt(numR * numS)).toInt

        // Calculate horizontal and vertical boundaries
        // Note: Each bucket contains elements in (leftBound, rightBound]
        horizontalBoundaries = rdd1.takeSample(withReplacement = false, cr).distinct.sorted
        verticalBoundaries = rdd2.takeSample(withReplacement = false, cs).distinct.sorted

        val maxInt = new Array[Int](1)
        maxInt(0) = Int.MaxValue
        horizontalBoundaries = horizontalBoundaries ++ maxInt
        verticalBoundaries = verticalBoundaries ++ maxInt

        val horizontalLength = horizontalBoundaries.length
        val verticalLength = verticalBoundaries.length

        // Calculate horizontal counts
        val mapper = (x: Int, c: Int) => for (i <-x until c) yield (i, 1)

        val horizontalC = rdd1.map(x => (horizontalBoundaries.indexWhere(v => x <= v), 1))
                                .flatMap(v => mapper(v._1, horizontalLength))    //Added to keep quantiles in horizontalCounts
                                .reduceByKey((v1, v2) => v1 + v2)                //instead of number of elements per bucket
                                .takeOrdered(cr + 1)

        horizontalCounts = Array.fill(horizontalLength)(0)

        horizontalC.foreach(x => horizontalCounts(x._1) = x._2)


        // Calculate vertical counts
        val verticalC = rdd2.map(x => (verticalBoundaries.indexWhere(v => x <= v), 1))
                            .flatMap(v => mapper(v._1, verticalLength))    //Added to keep quantiles in verticalCounts
                            .reduceByKey((v1, v2) => v1 + v2)              //instead of number of elements per bucket
                            .takeOrdered(cs + 1)

        verticalCounts = Array.fill(verticalLength)(0)

        verticalC.foreach(x => verticalCounts(x._1) = x._2)

        // Apply M-Bucket-I algorithm
        val buckets = mBucketI(op).zipWithIndex  //We add an index to each bucket, which we will later use as key,
                                                      //for the values that fall in the bucket

        // Mapper2 takes tuples of (value, indexInRDD) and returns an array of tuples (bucketIndex, (originalRDD, value)).
        // bucketIndex will be used as key, in order to send the value to the correct reducer.
        // Argument row is used to denote if the given value belongs to RDD1 (row == true) or RDD2 (row == false)
        val mapper2 = (x: (Int, Long), row: Boolean) => {

            var result = Array[(Long, (Int, Int))] ()

            if (row) {
                for (buck <- buckets) {

                    if (x._2 >= buck._1._1 && x._2 < buck._1._2) {

                        val newElement: (Long, (Int, Int)) =(buck._2,  (1, x._1))

                        result = result :+ newElement
                    }
                }
            } else {

                for (buck <- buckets) {

                    if (x._2 >= buck._1._3 && x._2 < buck._1._4) {

                        val newElement: (Long, (Int, Int)) = (buck._2, (2, x._1))

                        result = result :+ newElement
                    }
                }
            }

            result
        }

        // Map each value or rdd1 and rdd2 to the appropriate buckets, calculated by M-Bucket-I
        val rdd1WithKeys = rdd1.sortBy(x => x, ascending = true).zipWithIndex()
                            .flatMap(x => mapper2(x, true))

        val rdd2WithKeys = rdd2.sortBy(x => x, ascending = true).zipWithIndex()
                            .flatMap(x => mapper2(x, false))

        // Take the union of the two datasets
        val union = rdd1WithKeys++rdd2WithKeys

        // Partition the dataset by key and perform join
        val groups = union.groupByKey(numPartitions = reducers)
        groups.flatMap(x => joiner(x._2, op))
    }


    /* HELPER METHODS */

    /*
     * Takes an iterable of tuples (Int, Int) where element ._1 is the original rdd (either 1 or 2) and element ._2 is
     * the value. It splits the iterable into two arrays, according to element ._1 of each tuple.
     */
    def joiner(input: Iterable[(Int, Int)], op: String): Iterator[(Int, Int)] = {

        var tuples1 = Array[Int]()
        var tuples2 = Array[Int]()

        for (tuple <- input) {

            if (tuple._1 == 1) tuples1 = tuples1 :+ tuple._2
            else tuples2 = tuples2 :+ tuple._2
        }

        local_thetajoin(tuples1.iterator, tuples2.iterator, op)
    }


    /*
    * This method takes as input two lists of values that belong to the same partition
    * and performs the theta join on them.
    * */

    def local_thetajoin(dat1:Iterator[Int], dat2:Iterator[Int], op:String) : Iterator[(Int, Int)] = {
        var res = List[(Int, Int)]()
        var dat2List = dat2.toList

        while(dat1.hasNext) {
            val row1 = dat1.next()
            for(row2 <- dat2List) {
                if(checkCondition(row1, row2, op)) {
                    res = res :+ (row1, row2)
                }
            }
        }
        res.iterator
    }

    // M-Bucket-I algorithm
    def mBucketI(op: String): Array[(Long, Long, Long, Long)] = {

        var result = Array[(Long, Long, Long, Long)]()

        var row:Long = 0
        while (row < numR) {

            val (bestRow, subMatrix) = coverSubMatrix(row, op)

            val newRegions: Array[(Long, Long, Long, Long)] = subMatrix.map(x => (row, bestRow, x._1, x._2))
            result = result ++ newRegions

            row = bestRow
        }

        result
    }

    def coverSubMatrix(rowR: Long, op: String): (Long, Array[(Long, Long)]) = {

        var maxScore: Long = -1
        var bestRow: Long = rowR + math.min(bucketsize, numR - rowR)
        var bestRegions = Array[(Long, Long)]()

        var i = 1
        while (i <= math.min(bucketsize, numR - rowR)) {

            val Ri = coverRows(rowR, rowR + i, bucketsize / i, op)

            if (Ri.length != 0) {

                val area:Long = Ri.map(x => x._3).sum
                val score = area / Ri.length

                if (score >= maxScore) {

                    maxScore = score
                    bestRow = rowR + i
                    bestRegions = Ri.map(x => (x._1, x._2))
                }
            }

            // Use only divisors of bucket size as values of i
            i += 1
            while ((bucketsize % i != 0) && (i <= bucketsize)) i += 1
        }

        (bestRow, bestRegions)
    }

    /* maxNumOfColumns: max number of columns in each region
     * Returns an array of tuples that represent the vertical boundaries of each bucket, together with the number of
     * candidate cells in the bucket. Bucket boundaries are sets of the type [leftBound, rightBound).
     */
    def coverRows(rowF: Long, rowL: Long, maxNumOfColumns: Long, op: String): Array[(Long, Long, Long)] = {

        var columnBoundaries = Array[(Long, Long, Long)]()
        var r: (Long, Long, Long) = (0, 0, 0)

        // For all columns of the selected row region...
        var ci: Long = 1
        while (ci <= numS) {

            // ...enlarge region by one column
            r = (r._1, ci, 0)

            // If region contains maximum number of columns or we have reached the last column...
            if ((r._2 - r._1 >= maxNumOfColumns) || (ci == numS)) {

                //... if it contains at least one candidate cell, add it to columnRegions...
                val candCells = numCandidateCells(rowF, rowL, r._1, r._2, op)

                if (candCells > 0) {
                    r = (r._1, r._2, candCells)
                    columnBoundaries = columnBoundaries :+ r
                }

                //... and create a new columnRegion
                r = (ci, ci, 0)
            }
            ci += 1
        }

        // Return columnRegions for the requested rowRegions
        columnBoundaries
    }

    /*
     * Calculates the number of candidate cells in the given region
     */
    def numCandidateCells(rowF: Long, rowL: Long, columnF: Long, columnL: Long, op:String): Long = {

        var result: Long = 0

        var i = rowF
        while (i < rowL) {

            var j = columnF
            while (j < columnL) {

                if (isCandidateCell(i, j, op)) result += 1
                j += 1
            }
            i += 1
        }

        result
    }

    /*
     * Takes a cell, calculates the buckets it falls in, in the two histograms, and returns true if the cell is a
     * candidate cell according to the given operator
     */
    def isCandidateCell(row: Long, column: Long, op: String): Boolean = {

        val (x1, y1) = findHistBucket(row, horizontalCounts, horizontalBoundaries)
        val (x2, y2) = findHistBucket(column, verticalCounts, verticalBoundaries)

        op match {
            case "="  => if ((y1 <= x2) || (y2 <= x1)) false else true
            case "<"  => if      (x1 >= y2 - 1)        false else true
            case "<=" => if        (x1 >= y2)          false else true
            case ">"  => if      (y1 <= x2 + 1)        false else true
            case ">=" => if        (y1 <= x2)          false else true
            case "!=" => if ((x1 == x2) && (y1 == y2)
                                    && (y1 - x1 == 1)) false else true
        }
    }


    /*
     * Takes the row or column number of a cell and returns the boundaries of the bucket it falls in, in the
     * corresponding equi-depth histogram
     */
    def findHistBucket(id: Long, counts: Array[Int], boundaries: Array[Int]): (Int, Int) = {

        var result = (0, 0)

        val index = counts.indexWhere(v => id < v)

        if (index == 0) result = (Int.MinValue, boundaries(index))
        else result = (boundaries(index - 1), boundaries(index))

        result
    }


    def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
        op match {
            case "="  => value1 == value2
            case "<"  => value1 < value2
            case "<=" => value1 <= value2
            case ">"  => value1 > value2
            case ">=" => value1 >= value2
            case "!=" => value1 != value2
        }
    }


    // Helping printing method
    def printM(op: String): Unit = {

        var i = 0
        while(i < numR) {
            var j = 0
            while(j < numS) {
                if (isCandidateCell(i, j, op)) print("true  ") else print("false ")
                j += 1
            }
            println()
            i += 1
        }
    }
}


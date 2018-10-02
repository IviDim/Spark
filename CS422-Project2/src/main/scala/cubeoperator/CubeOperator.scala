package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

    /*
    * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
    * the attribute on which the aggregation is performed
    * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
    * and returns an RDD with the result in the form of <key = string, value = double> pairs.
    * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
    * You are free to do that following your own naming convention.
    * The value is the aggregation result.
    * You are not allowed to change the definition of this function or the names of the aggregate functions.
    * */
    def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

        val rdd = dataset.getRDD()
        val schema = dataset.getSchema()

        val index = groupingAttributes.map(x => schema.indexOf(x))  //Starting from 0
        val indexAgg = schema.indexOf(aggAttribute)

        agg match {

            case "AVG" =>

                // FIRST PHASE:
                //--------------

                // Map stage: Map each row of rdd to the corresponding (key, value) pair
                val mapStage = rdd.map(row => (index.map(i => row(i)), (row.getInt(indexAgg), 1)))

                // Combine + Reduce stages: Apply reduceByKey and various map calls, to produce bottom cell of lattice
                // and partial results.
                // Function reduceByKey does both the Combine and the Reduce stage of the first phase of the algorithm,
                // according to the Spark documentation
                val reducer = (combiner: (Int, Int), value: (Int, Int)) => (combiner._1 + value._1, combiner._2 + value._2)

                val reduceStage = mapStage.reduceByKey(reducer, numPartitions = reducers)
                                    .map(pair => (Functions.combinations(pair._1), pair._2))
                                    .flatMap(pair => pair._1.map(comb => (comb, pair._2)))

                // SECOND PHASE:
                //---------------

                // Combine + Reduce stages: Apply reduceByKey to get the final cube
                val reduceStage2 = reduceStage.reduceByKey(reducer, numPartitions = reducers).map(pair => (pair._1.mkString(";"),
                                                                                pair._2._1.toDouble / pair._2._2))

                // Return final cube
                reduceStage2

            case _ =>

                // FIRST PHASE:
                //--------------

                // Map stage: Map each row of rdd to the corresponding (key, value) pair
                val mapStage = agg match {

                    case "COUNT" => rdd.map(row => (index.map(i => row(i)), 1))
                    case _ => rdd.map(row => (index.map(i => row(i)), row.getInt(indexAgg)))
                }

                // Combine + Reduce stages: Apply reduceByKey and various map calls, to produce bottom cell of lattice
                // and partial results.
                // Function reduceByKey does both the Combine and the Reduce stage of the first phase of the algorithm,
                // according to the Spark documentation
                val reducer = agg match {

                    case "MAX" => (combiner: Int, value: Int) => if (combiner > value) combiner else value
                    case "MIN" => (combiner: Int, value: Int) => if (combiner < value) combiner else value
                    case _     => (combiner: Int, value: Int) => combiner + value
                }

                val reduceStage = mapStage.reduceByKey(reducer, numPartitions = reducers)
                                    .map(pair => (Functions.combinations(pair._1), pair._2))
                                    .flatMap(pair => pair._1.map(comb => (comb, pair._2)))

                // SECOND PHASE:
                //---------------

                // Combine + Reduce stages: Apply reduceByKey to get the final cube
                val reduceStage2 = reduceStage.reduceByKey(reducer, numPartitions = reducers).map(pair => (pair._1.mkString(";"), pair._2.toDouble))

                // Return final cube
                reduceStage2

        }
    }

    def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

        //Naive algorithm for cube computation

        val rdd = dataset.getRDD()
        val schema = dataset.getSchema()

        val index = groupingAttributes.map(x => schema.indexOf(x))  //Starting from 0
        val indexAgg = schema.indexOf(aggAttribute)

        val reduceStage = agg match {

            case "AVG" =>

                val reducer = (combiner: (Int, Int), value: (Int, Int)) => (combiner._1 + value._1, combiner._2 + value._2)

                val initialTuples = rdd.map(row => (index.map(i => row(i)), (row.getInt(indexAgg), 1)))
                val mapStage = initialTuples.map(pair => (Functions.combinations(pair._1), pair._2))
                                            .flatMap(pair => pair._1.map(comb => (comb, pair._2)))
                mapStage.reduceByKey(reducer, numPartitions = reducers).map(pair => (pair._1.mkString(";"), pair._2._1.toDouble / pair._2._2))
            case _ =>

                val initialTuples = agg match {
                    case "COUNT" => rdd.map(row => (index.map(i => row(i)), 1))
                    case _ => rdd.map(row => (index.map(i => row(i)), row.getInt(indexAgg)))
                }

                val mapStage = initialTuples.map(pair => (Functions.combinations(pair._1), pair._2))
                                           .flatMap(pair => pair._1.map(comb => (comb, pair._2)))

                val reducer = agg match {

                    case "MAX" => (combiner: Int, value: Int) => if (combiner > value) combiner else value
                    case "MIN" => (combiner: Int, value: Int) => if (combiner < value) combiner else value
                    case _     => (combiner: Int, value: Int) => combiner + value
                }

                mapStage.reduceByKey(reducer, numPartitions = reducers).map(pair => (pair._1.mkString(";"), pair._2.toDouble))
        }

        reduceStage
    }
}


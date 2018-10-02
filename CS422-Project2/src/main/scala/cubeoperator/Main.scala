package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.log4j.PropertyConfigurator

object Main {
    def main(args: Array[String]) {

        PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))

        val reducers = 10

        val inputFile= "/project2/lineorder_small.tbl"  //For docker
//        val inputFile= "target/scala-2.11/test-classes/lineorder_small.tbl"

        val outputRegular = "output-regular"
        val outputNaive = "output-naive"

        val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")  //TODO Comment setMaster
        val ctx = new SparkContext(sparkConf)
        val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

        val df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "|")
          .load(inputFile)

        val rdd = df.rdd

        val schema = df.schema.toList.map(x => x.name)

        val dataset = new Dataset(rdd, schema)

        val cb = new CubeOperator(reducers)

        var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate", "lo_custkey",  "lo_custkey",  "lo_custkey")

        // Regular Cube

        val t1 = System.nanoTime
        val res1 = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
        println("Smart results: " + res1.count)
        val t2 = System.nanoTime

        // Save res1 to file

//        res1.repartition(1).saveAsTextFile(outputRegular)

        // Naive Cube

        val t3 = System.nanoTime
        val res2 = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
        println("Naive results: " + res2.count)
        val t4 = System.nanoTime

        // Save res2 to file

//        res2.repartition(1).saveAsTextFile(outputNaive)

        //------------------------------------//

        //Perform the same query using SparkSQL
        /*
           The above call corresponds to the query:
           SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
           FROM LINEORDER
           CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
         */

        val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate", "lo_custkey",  "lo_custkey",  "lo_custkey")
                    .agg(sum("lo_supplycost") as "sum supplycost")
//        q1.show(100)

        println("\n")
        println("Smart time: " + (t2-t1)/Math.pow(10,9))
        println("Naive time: " + (t4-t3)/Math.pow(10,9))

//        if (res1.count() == q1.count) println("Smart correct! Time: " + (t2-t1)/Math.pow(10,9)) else println("Smart wrong!")
//        if (res2.count() == q1.count) println("Naive correct! Time: " + (t4-t3)/Math.pow(10,9)) else println("Naive wrong!")
    }
}
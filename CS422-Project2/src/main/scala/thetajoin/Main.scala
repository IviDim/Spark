package thetajoin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.log4j.PropertyConfigurator

object Main {
    def main(args: Array[String]) {

        PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))

        val reducers = 10  //10
        val maxInput = 1000  //1000

        val inputFile1="/project2/input1_1K.csv" //For cluster
        val inputFile2="/project2/input2_1K.csv" // For cluster

//        val inputFile1 = "src/test/resources/input1_2K.csv"
//        val inputFile2 = "src/test/resources/input2_2K.csv"

        val output = "output"

        val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")  //TODO Comment setMaster
        val ctx = new SparkContext(sparkConf)

        val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

        val df1 = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(inputFile1)

        val df2 = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(inputFile2)

        val rdd1 = df1.rdd
        val rdd2 = df2.rdd

        val schema1 = df1.schema.toList.map(x => x.name)
        val schema2 = df2.schema.toList.map(x => x.name)

        val dataset1 = new Dataset(rdd1, schema1)
        val dataset2 = new Dataset(rdd2, schema2)

        // Run theta-join algorithm

        val t1 = System.nanoTime
        val tj = new ThetaJoin(dataset1.getRDD().count, dataset2.getRDD.count, reducers, maxInput)
        val res = tj.theta_join(dataset1, dataset2, "num", "num", "=")

        val resultSize = res.count
        val t2 = System.nanoTime

        // Run theta join with cartesian product

        val index1 = schema1.indexOf("num")
        val index2 = schema2.indexOf("num")

        val cartRes = rdd1.cartesian(rdd2).flatMap(x => {
            val v1 = x._1(index1).asInstanceOf[Int]
            val v2 = x._2(index2).asInstanceOf[Int]
            if(v1 == v2)
                List((v1, v2))
            else
                List()
        })

        val resultSizeCartesian = cartRes.count

        if (resultSize == resultSizeCartesian) println("Correct!")
        else println("False: " + resultSize + " different than " + resultSizeCartesian)

        println("Time: " + (t2-t1) / Math.pow(10,9))

//        res.sortBy(x => x).repartition(numPartitions = 1).saveAsTextFile(output)
    }
}

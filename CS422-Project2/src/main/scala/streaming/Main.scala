package streaming;

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.log4j.PropertyConfigurator

object Main {
  def main(args: Array[String]) {
    PropertyConfigurator.configure(getClass.getResourceAsStream("/log4j.properties"))
    val output = "output"
    val sparkConf = new SparkConf().setAppName("CS422-Project2-Task3") //.setMaster("local[16]")

    val streaming = new SparkStreaming(sparkConf, args);

    streaming.consume();
  }
}

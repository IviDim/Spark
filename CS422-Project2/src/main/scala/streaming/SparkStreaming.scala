package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._

import scala.collection.Seq
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.net.URI

import org.apache.spark.util.sketch.CountMinSketch

import scala.util.Random
import scala.util.hashing.{MurmurHash3=>MH3}

class SparkStreaming(sparkConf: SparkConf, args: Array[String]){

    val sparkconf = sparkConf;

    if (args.length < 4) {
        println("Too few arguments!")
        println("Program takes at least 4 arguments: input directory, num of seconds per window, top-K, execution type")
        sys.exit(-1)
    }

    // get the directory in which the stream is filled.
    val inputDirectory = args(0)

    // number of seconds per window
    val seconds = args(1).toInt;

    // K: number of heavy hitters stored
    val TOPK = args(2).toInt;

    // precise Or approx
    val execType = args(3);

    if (execType.contains("approx") && (args.length < 8)) {

        println("Too few arguments!")
        println("Execution type \"approx\" takes arguments: IP address 1, IP address 2, epsilon, delta")
        sys.exit(-1)
    }

    //  create a StreamingContext, the main entry point for all streaming functionality.
    val ssc = new StreamingContext(sparkConf, Seconds(seconds));
    ssc.checkpoint("/checkpoint")   // set checkpoint directory

    def consume() {

        // create a DStream that represents streaming data from a directory source.
        val linesDStream = ssc.textFileStream(inputDirectory);

        // parse the stream. (line -> (IP1, IP2))
        val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

        if (execType.contains("precise")) {

            val precTopK = PreciseTopK(TOPK)

            // Count frequency of IP pairs for this batch
            val pairs = words.map(word => (word, 1))

            val reducer = (v1: Int, v2: Int) => v1 + v2
            val localPairCounts = pairs.reduceByKey(reducer)

            localPairCounts.foreachRDD { rdd =>
                val byCount = rdd.map(x => x.swap)
                val topK = byCount.top(precTopK.topK)

                println("This batch: [" + topK.map(v => v.toString).mkString(",") + "]")
            }

            // Update global frequency of IP pairs
            val globalPairCounts = localPairCounts.updateStateByKey(Functions.updateFunction _)

            globalPairCounts.foreachRDD { rdd =>
                val byCount = rdd.map(x => x.swap)
                val topK = byCount.top(precTopK.topK)

                println("Global: [" + topK.map(v => v.toString).mkString(",") + "]")
            }

        } else if (execType.contains("approx")) {

            //Setup CM Sketch attributes
            val ip1 = args(4)
            val ip2 = args(5)
            val monitoredPair = ip1 + "," + ip2
            println("Monitored IPs: (" + monitoredPair + ")")
            val epsilon = args(6).toDouble
            val delta = args(7).toDouble

            val w = math.ceil(math.E / epsilon).toInt     //Num of columns in CM Sketch
            val d = math.ceil(math.log(1 / delta)).toInt  //Num of rows in CM Sketch

            val rand = new Random()
            val seeds: Array[Int] = new Array[Int](d)
            for (i <- 0 until d) {
                seeds(i) = rand.nextInt()
            }

            val approxTopK = ApproxTopK(monitoredPair)
            // Pass monitored pair by all the hash functions, to find the cells of the CMSketch that correspond to the pair
            approxTopK.mask = seeds.map(seed => Math.abs(MH3.stringHash(approxTopK.monitoredPair, seed) % w))
                                    .zipWithIndex.map(_.swap)

            // Count frequency of IP pairs for this batch
            val pairs = words.map(word => (word, 1))

            val reducer = (v1: Int, v2: Int) => v1 + v2
            val localPairCounts = pairs.reduceByKey(reducer)

            // Compute CM Sketch for this batch
            val localCMSketch = localPairCounts.map(v => (v._1.productIterator.mkString(","), v._2))
                    .map(v => (seeds.map(seed => Math.abs(MH3.stringHash(v._1, seed) % w)).zipWithIndex, v._2))
                    .flatMap(v => v._1.map(_.swap).map(x => (x, v._2)))
                    .reduceByKey(reducer)

            // Calculate frequency of monitored pair, for this batch, from CM Sketch
            localCMSketch.foreachRDD { rdd =>

                val localFreq = rdd.filter(v => approxTopK.mask.contains(v._1))
                if (localFreq.count < d) {

                    // If the monitored pair was not present in the current batch (a.k.a. if not ALL cells that correspond
                    // to the pair, have a value), print []

                    println("This batch: []")
                } else {

                    val result = localFreq.reduce((v1, v2) => if (v1._2 < v2._2) v1 else v2)
                    println("This batch: [(" + result._2 + ",(" + approxTopK.monitoredPair + "))]")
                }
            }

            // Update global CM Sketch
            val globalCMSketch = localCMSketch.updateStateByKey(Functions.updateFunction _)

            // Calculate frequency of monitored pair, globally, from CM Sketch
            globalCMSketch.foreachRDD { rdd =>

                val globalFreq = rdd.filter(v => approxTopK.mask.contains(v._1))
                if (globalFreq.count < d) {

                    // If the monitored pair has never been captured, (a.k.a. if not ALL cells that correspond
                    // to the pair, have a value), print []

                    println("Global: []")
                } else {

                    val result = globalFreq.reduce((v1, v2) => if (v1._2 < v2._2) v1 else v2)
                    println("Global: [(" + result._2 + ",(" + approxTopK.monitoredPair + "))]")
                }
            }
        }

        // Start the computation
        ssc.start()

        // Wait for the computation to terminate
        ssc.awaitTermination()
    }
}
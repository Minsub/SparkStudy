package minsub.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.streaming._

import scala.collection.mutable.Queue

object FileStream {

  val PATH = "src/main/resources/"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming")
    val ssc = new StreamingContext(conf, Seconds(1))

    val fileStream = ssc.textFileStream(PATH+"static")

    val wordStream = fileStream.flatMap(_.split(" ")).map((_,1))
    val countStream = wordStream.reduceByKey(_+_)

    countStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

package minsub.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming WordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordCount = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(_+_, Seconds(5))

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    //nc -lk 9999
  }

}

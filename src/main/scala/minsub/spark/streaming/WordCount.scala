package minsub.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming")
    val ssc = new StreamingContext(conf, Seconds(2))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val count = pairs.reduceByKey(_+_)

    count.print()

    ssc.start()
    ssc.awaitTermination()

    //nc -lk 9999
  }

}

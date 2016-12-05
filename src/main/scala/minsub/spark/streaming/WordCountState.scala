package minsub.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object WordCountState {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming")
    val ssc = new StreamingContext(conf, Seconds(1))


    val initialRDD = ssc.sparkContext.parallelize(List(("Hello",1),("World",1)))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // ReduceByKey(_+_)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    ssc.sparkContext.setCheckpointDir("src/main/resources/static/checkpoint/streaming/state")

    val stateDstream = wordDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()

    //nc -lk 9999
  }
}

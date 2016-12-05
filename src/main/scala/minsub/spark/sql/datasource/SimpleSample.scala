package minsub.spark.sql.datasource

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SimpleSample {

  case class Person(name:String, age:String)

  def main(args: Array[String]) = {
    // Create Spark SQL
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val peopleDF = spark.read.format("json").load("src/main/resources/static/dataset.json")
    peopleDF.select("name", "age").filter("age = 21").write.format("parquet").save("src/main/resources/static/out/people.parquet")

    println("COMPLETE SAVE to static/out/people.parquet")
  }
}

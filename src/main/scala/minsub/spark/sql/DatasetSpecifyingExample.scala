package minsub.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DatasetSpecifyingExample {

  case class Person(name:String, age:String)

  def main(args: Array[String]) = {
    // Create Spark SQL
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSQL sample4")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val peopleRDD = spark.sparkContext.textFile("src/main/resources/static/people.txt")
    val schemaString = "name age"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attr => Row(attr(0), attr(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")

    val peopleDS = spark.sql("SELECT * FROM people")
    peopleDS.show()

  }
}

package minsub.spark.sql.datasource

import org.apache.spark.sql.SparkSession

object RunDirectFiles {

  case class Person(name:String, age:String)

  def main(args: Array[String]) = {
    // Create Spark SQL
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/static/out/people.parquet`")
    sqlDF.show()

    spark.sql("SELECT * FROM json.`src/main/resources/static/dataset.json`").show()
  }
}

package org.apache.spark.examples.sql

// $example on$
import org.apache.spark.sql.SparkSession
// $example off$

object SparkSessionExample {
  def main(args: Array[String]) {
    // $example on$
    val spark = SparkSession
      .builder()
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    // $example off$
  }
}

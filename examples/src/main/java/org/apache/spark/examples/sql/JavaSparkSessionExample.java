package org.apache.spark.examples.sql;

// $example on$
import org.apache.spark.sql.SparkSession;
// $example off$

public class JavaSparkSessionExample {
  public static void main(String[] args) {
    // $example on$
    SparkSession spark = SparkSession
      .builder()
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();
    // $example off$
  }
}

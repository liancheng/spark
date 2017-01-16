/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.avro

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession

// scalastyle:off println

/**
 * This object runs a simple benchmark test on the avro files in benchmarkFilesDir. It measures
 * how long does it take to convert them into DataFrame and run count() method on them. See
 * README on how to invoke it.
 */
object AvroReadBenchmark {

  def main(args: Array[String]) {
    val benchmarkDirFiles = new File(AvroFileGenerator.outputDir).list
    if (benchmarkDirFiles == null || benchmarkDirFiles.isEmpty) {
      sys.error(s"The benchmark directory ($AvroFileGenerator.outputDir) does not exist or " +
        "is empty. First you should generate some files to run a benchmark with (see README)")
    }

    val spark = SparkSession.builder().master("local[2]").appName("AvroReadBenchmark")
      .getOrCreate()

    spark.read.avro(AvroFileGenerator.outputDir).count()

    println("\n\n\nStaring benchmark test - creating DataFrame from benchmark avro files\n\n\n")

    val startTime = System.nanoTime
    spark
      .read
      .avro(AvroFileGenerator.outputDir)
      .select("string")
      .count()
    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    spark.sparkContext.stop()  // Otherwise scary exception message appears
  }
}

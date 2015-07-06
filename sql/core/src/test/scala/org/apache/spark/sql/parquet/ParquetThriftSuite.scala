package org.apache.spark.sql.parquet

import scala.collection.JavaConversions._

import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.thrift.ParquetThriftOutputFormat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.parquet.thrift.SampleThriftObject
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.rdd.RDD._

class ParquetThriftSuite extends SparkFunSuite with ParquetTest {
  override def sqlContext: SQLContext = TestSQLContext

  test("thrift") {
    val values = (0 until 10).map { i =>
      new SampleThriftObject(i, s"val_i", seqAsJavaList(Seq(i, i + 1)))
    }

    val job = new Job()
    ParquetThriftOutputFormat.setThriftClass(job, classOf[SampleThriftObject])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[SampleThriftObject])

    sqlContext
      .sparkContext
      .parallelize(values)
      .coalesce(1)
      .map((null, _))
      .saveAsNewAPIHadoopFile(
        "file:/tmp/foo",
        classOf[Void],
        classOf[SampleThriftObject],
        classOf[ParquetThriftOutputFormat[SampleThriftObject]],
        job.getConfiguration)
  }
}

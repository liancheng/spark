/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * This suite performs basic integration tests where the AWS credentials have been
 * encoded into the tempdir URI rather than being set in the Hadoop configuration.
 */
@ExtendedRedshiftTest
class AWSCredentialsInUriIntegrationSuite extends IntegrationSuiteBase {

  override protected val tempDir: String = {
    val uri = new URI(AWS_S3_SCRATCH_SPACE + randomSuffix + "/")
    new URI(
      uri.getScheme,
      s"$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY",
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment).toString
  }


  // Override this method so that we do not set the credentials in sc.hadoopConf.
  override def beforeAll(): Unit = {
    assert(tempDir.contains("AKIA"), "tempdir did not contain AWS credentials")
    assert(!AWS_SECRET_ACCESS_KEY.contains("/"), "AWS secret key should not contain slash")
    sc = new SparkContext("local", getClass.getSimpleName)
    conn = DefaultJDBCWrapper.getConnector(None, jdbcUrl, None)
  }

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    testRoundtripSaveAndLoad(s"roundtrip_save_and_load_$randomSuffix", df)
  }
}

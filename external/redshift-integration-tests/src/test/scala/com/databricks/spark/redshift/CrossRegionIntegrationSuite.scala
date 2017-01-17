/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * Integration tests where the Redshift cluster and the S3 bucket are in different AWS regions.
 */
@ExtendedRedshiftTest
class CrossRegionIntegrationSuite extends IntegrationSuiteBase {

  protected val AWS_S3_CROSS_REGION_SCRATCH_SPACE: String =
    loadConfigFromEnv("AWS_S3_CROSS_REGION_SCRATCH_SPACE")
  require(AWS_S3_CROSS_REGION_SCRATCH_SPACE.contains("s3n"), "must use s3n:// URL")

  override protected val tempDir: String = AWS_S3_CROSS_REGION_SCRATCH_SPACE + randomSuffix + "/"

  test("write") {
    val bucketRegion = Utils.getRegionForS3Bucket(
      tempDir,
      new AmazonS3Client(new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))).get
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(df)
        .option("dbtable", tableName)
        .option("extracopyoptions", s"region '$bucketRegion'")
        .save()
      // Check that the table exists. It appears that creating a table in one connection then
      // immediately querying for existence from another connection may result in spurious "table
      // doesn't exist" errors; this caused the "save with all empty partitions" test to become
      // flaky (see #146). To work around this, add a small sleep and check again:
      if (!DefaultJDBCWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      }
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}

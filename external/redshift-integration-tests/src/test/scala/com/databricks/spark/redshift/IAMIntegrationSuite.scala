/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.sql.SQLException

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * Integration tests for configuring Redshift to access S3 using Amazon IAM roles.
 */
@ExtendedRedshiftTest
class IAMIntegrationSuite extends IntegrationSuiteBase {

  private val IAM_ROLE_ARN: String = loadConfigFromEnv("STS_ROLE_ARN")

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    withTempRedshiftTable("iam_roundtrip_save_and_load") { tableName =>
      write(df)
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("aws_iam_role", IAM_ROLE_ARN)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = read
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("aws_iam_role", IAM_ROLE_ARN)
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    }
  }

  test("load fails if IAM role cannot be assumed") {
    withTempRedshiftTable("iam_load_fails_if_role_cannot_be_assumed") { tableName =>
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
      val err = intercept[SQLException] {
        write(df)
          .option("dbtable", tableName)
          .option("forward_spark_s3_credentials", "false")
          .option("aws_iam_role", IAM_ROLE_ARN + "-some-bogus-suffix")
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(err.getCause.getMessage.contains("is not authorized to assume IAM Role"))
    }
  }
}

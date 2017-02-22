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
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * Integration tests for accessing S3 using Amazon Security Token Service (STS) credentials.
 */
@ExtendedRedshiftTest
class STSIntegrationSuite extends IntegrationSuiteBase {

  private val STS_ROLE_ARN: String = loadConfigFromEnv("STS_ROLE_ARN")
  private var STS_ACCESS_KEY_ID: String = _
  private var STS_SECRET_ACCESS_KEY: String = _
  private var STS_SESSION_TOKEN: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    val stsClient = new AWSSecurityTokenServiceClient(awsCredentials)
    val assumeRoleRequest = new AssumeRoleRequest()
    assumeRoleRequest.setDurationSeconds(900) // this is the minimum supported duration
    assumeRoleRequest.setRoleArn(STS_ROLE_ARN)
    assumeRoleRequest.setRoleSessionName(s"spark-$randomSuffix")
    val creds = stsClient.assumeRole(assumeRoleRequest).getCredentials
    STS_ACCESS_KEY_ID = creds.getAccessKeyId
    STS_SECRET_ACCESS_KEY = creds.getSecretAccessKey
    STS_SESSION_TOKEN = creds.getSessionToken
  }

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    withTempRedshiftTable("roundtrip_save_and_load") { tableName =>
      write(df)
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("temporary_aws_access_key_id", STS_ACCESS_KEY_ID)
        .option("temporary_aws_secret_access_key", STS_SECRET_ACCESS_KEY)
        .option("temporary_aws_session_token", STS_SESSION_TOKEN)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = read
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("temporary_aws_access_key_id", STS_ACCESS_KEY_ID)
        .option("temporary_aws_secret_access_key", STS_SECRET_ACCESS_KEY)
        .option("temporary_aws_session_token", STS_SESSION_TOKEN)
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    }
  }
}

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
import java.sql.Connection

import scala.util.Random

import com.databricks.sql.DatabricksSQLConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3native.NativeS3FileSystem
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.StructType

/**
 * Base class for writing integration tests which run against a real Redshift cluster.
 */
trait IntegrationSuiteBase
  extends QueryTest
    with Matchers
    with BeforeAndAfterEach {

  protected def loadConfigFromEnv(envVarName: String): String = {
    Option(System.getenv(envVarName)).getOrElse {
      fail(s"Must set $envVarName environment variable")
    }
  }

  // The following configurations must be set in order to run these tests. In Travis, these
  // environment variables are set using Travis's encrypted environment variables feature:
  // http://docs.travis-ci.com/user/environment-variables/#Encrypted-Variables

  // JDBC URL listed in the AWS console (should not contain username and password).
  protected val AWS_REDSHIFT_JDBC_URL: String = loadConfigFromEnv("AWS_REDSHIFT_JDBC_URL")
  protected val AWS_REDSHIFT_USER: String = loadConfigFromEnv("AWS_REDSHIFT_USER")
  protected val AWS_REDSHIFT_PASSWORD: String = loadConfigFromEnv("AWS_REDSHIFT_PASSWORD")
  protected val AWS_ACCESS_KEY_ID: String = loadConfigFromEnv("TEST_AWS_ACCESS_KEY_ID")
  protected val AWS_SECRET_ACCESS_KEY: String = loadConfigFromEnv("TEST_AWS_SECRET_ACCESS_KEY")
  // Path to a directory in S3 (e.g. 's3n://bucket-name/path/to/scratch/space').
  protected val AWS_S3_SCRATCH_SPACE: String = loadConfigFromEnv("AWS_S3_SCRATCH_SPACE")
  require(AWS_S3_SCRATCH_SPACE.contains("s3n"), "must use s3n:// URL")

  protected def jdbcUrl: String = {
    s"$AWS_REDSHIFT_JDBC_URL?user=$AWS_REDSHIFT_USER&password=$AWS_REDSHIFT_PASSWORD"
  }

  /**
   * Random suffix appended appended to schema, table and directory names to avoid collisions
   * between separate Travis builds.
   */
  protected val randomSuffix: String = Math.abs(Random.nextLong()).toString

  protected val tempDir: String = AWS_S3_SCRATCH_SPACE + randomSuffix + "/"

  protected val schemaName = s"redshift_it_$randomSuffix"

  protected val createSqlContextBeforeEach = true

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var conn: Connection = _
  protected var sparkSession: SparkSession = _

  def jdbcUpdate(query: String): Unit = {
    log.debug("JDBC RUNNING: " + Utils.sanitizeQueryText(query))
    conn.createStatement().executeUpdate(query)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")
    // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
    sc.hadoopConfiguration.setBoolean("fs.s3.impl.disable.cache", true)
    sc.hadoopConfiguration.setBoolean("fs.s3n.impl.disable.cache", true)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    conn = DefaultJDBCWrapper.getConnector(None, jdbcUrl, None)
    conn.setAutoCommit(true)
    jdbcUpdate(s"create schema if not exists $schemaName")
    jdbcUpdate(s"set search_path to $schemaName, '$$user', public")
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop schema if exists $schemaName cascade")
      val conf = new Configuration(false)
      conf.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
      conf.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
      // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
      conf.setBoolean("fs.s3.impl.disable.cache", true)
      conf.setBoolean("fs.s3n.impl.disable.cache", true)
      conf.set("fs.s3.impl", classOf[NativeS3FileSystem].getCanonicalName)
      conf.set("fs.s3n.impl", classOf[NativeS3FileSystem].getCanonicalName)
      val fs = FileSystem.get(URI.create(tempDir), conf)
      fs.delete(new Path(tempDir), true)
      fs.close()
    } finally {
      try {
        conn.close()
      } finally {
        try {
          sc.stop()
        } finally {
          super.afterAll()
        }
      }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    if (createSqlContextBeforeEach) {
      sqlContext = new TestHiveContext(sc, loadTestTables = false)
      sparkSession = sqlContext.sparkSession
    }
  }

  /**
   * Create a new DataFrameReader using common options for reading from Redshift.
   */
  protected def read: DataFrameReader = {
    sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", tempDir)
      .option("forward_spark_s3_credentials", "true")
      .option("search_path", s"$schemaName, '$$user', public")
  }
  /**
   * Create a new DataFrameWriter using common options for writing to Redshift.
   */
  protected def write(df: DataFrame): DataFrameWriter[Row] = {
    df.write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", tempDir)
      .option("forward_spark_s3_credentials", "true")
      .option("search_path", s"$schemaName, '$$user', public")
  }

  protected def createTestDataInRedshift(tableName: String): Unit = {
    jdbcUpdate(s"drop table if exists $tableName")
    jdbcUpdate(
      s"""
         |create table $tableName (
         |testbyte int2,
         |testbool boolean,
         |testdate date,
         |testdouble float8,
         |testfloat float4,
         |testint int4,
         |testlong int8,
         |testshort int2,
         |teststring varchar(256),
         |testtimestamp timestamp
         |)
      """.stripMargin
    )
    // scalastyle:off
    jdbcUpdate(
      s"""
         |insert into $tableName values
         |(null, null, null, null, null, null, null, null, null, null),
         |(0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', '2015-07-03 00:00:00.000'),
         |(0, false, null, -1234152.12312498, 100000.0, null, 1239012341823719, 24, '___|_123', null),
         |(1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000'),
         |(1, true, '2015-07-01', 1234152.12312498, 1.0, 42, 1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001'),
         |(null, null, null, null, null, null, null, null, 'Ba\\\\ckslash\\\\', null)
         """.stripMargin
    )
    // scalastyle:on
  }

  protected def withTempRedshiftTable[T](namePrefix: String)(body: String => T): T = {
    val tableName = s"$namePrefix$randomSuffix"
    try {
      body(tableName)
    } finally {
      jdbcUpdate(s"drop table if exists $tableName")
    }
  }

  /**
   * Runs `df` twice, once with Basic Filter Pushdown and once with Advanced Pushdown enabled.
   * Uses `checkAnswer()` to compare both results to the `expectedAnswer`
   *
   * @return A pair of SQL statements produced by the basic and advanced pushdown code paths.
   */
  def checkAnswerPushdown(df: DataFrame, expectedAnswer: Seq[Row]): (String, String) = {
    def check(advancedPushdown: Boolean): String = {
      if (advancedPushdown) {
        enablePushdownSession(df.sparkSession)
      } else {
        disablePushdownSession(df.sparkSession)
      }
      // We put an alias on top to trigger full re-execution of the whole df.
      val aliasedDF = df.as(s"AdvancedPD_$advancedPushdown")
      checkAnswer(aliasedDF, expectedAnswer)
      val firstSelect = Utils.getFirstSelect(aliasedDF)
      assert(firstSelect.isDefined)
      firstSelect.get
    }
    (check(advancedPushdown = false), check(advancedPushdown = true))
  }

  /**
   * Verify that the pushdown was done by looking at the generated SQL,
   * and check the results are as expected
   */
  def testPushdownDF(result: DataFrame, refResult: Seq[Row],
      refBasicSQL: Option[String] = None,
      refFullSQL: Option[String] = None): Unit = {
    val (basicSQL, fullSQL) = checkAnswerPushdown(result, refResult)

    def cleanupSQL(sql: String): String =
      Utils.prettyPrint(sql.trim.replaceAll(s"_$randomSuffix|\\s+", " ")
        // FIXME: Order of cols is nondeterministic. See SC-5957.
        .replaceFirst(s"(?ims)(SELECT )(.*)( FROM)", "$1 FIXME $3")
      )

    def checkSQL(sql: String, refSQL: String): Unit = {
      val generatedClean = cleanupSQL(sql)
      val referenceClean = cleanupSQL(refSQL)
      val generatedNoWS = generatedClean.replaceAll("\\s", "")
      val referenceNoWS = referenceClean.replaceAll("\\s", "")
      if (generatedNoWS != referenceNoWS) {
        // scalastyle:off println
        println(
          s"""
             |>>> GENERATED QUERY:
             |$generatedClean
             |
             |>>> REFERENCE QUERY:
             |$referenceClean
             |
           """.stripMargin
        )
        disablePushdownSession(result.sparkSession)
        println(">>> Explain (disablePushdown):")
        result.explain(true)
        enablePushdownSession(result.sparkSession)
        println(">>> Explain (enablePushdown):")
        result.explain(true)
        // scalastyle:on println

        assertResult(referenceNoWS)(generatedNoWS)
      }
    }

    if (refBasicSQL.isDefined) {
      checkSQL(basicSQL, refBasicSQL.get)
    }
    if (refFullSQL.isDefined) {
      checkSQL(fullSQL, refFullSQL.get)
    }
  }

  def testPushdownSQL(sql: String, refResult: Seq[Row],
      refBasicSQL: Option[String] = None,
      refFullSQL: Option[String] = None): Unit = {
    val result = sparkSession.sql(sql)
    testPushdownDF(result, refResult, refBasicSQL, refFullSQL)
  }

  def enablePushdownSession(session: SparkSession): Unit = {
    session.conf.set(DatabricksSQLConf.REDSHIFT_ADVANCED_PUSHDOWN.key, true)
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    session.conf.set(DatabricksSQLConf.REDSHIFT_ADVANCED_PUSHDOWN.key, false)
  }

  /**
   * Save the given DataFrame to Redshift, then load the results back into a DataFrame and check
   * that the returned DataFrame matches the one that we saved.
   *
   * @param tableName the table name to use
   * @param df the DataFrame to save
   * @param expectedSchemaAfterLoad if specified, the expected schema after loading the data back
   *                                from Redshift. This should be used in cases where you expect
   *                                the schema to differ due to reasons like case-sensitivity.
   * @param saveMode the [[SaveMode]] to use when writing data back to Redshift
   */
  def testRoundtripSaveAndLoad(
    tableName: String,
    df: DataFrame,
    expectedSchemaAfterLoad: Option[StructType] = None,
    saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    withTempRedshiftTable(tableName) { tableName =>
      write(df)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
      // Check that the table exists. It appears that creating a table in one connection then
      // immediately querying for existence from another connection may result in spurious "table
      // doesn't exist" errors; this caused the "save with all empty partitions" test to become
      // flaky (see #146). To work around this, add a small sleep and check again:
      if (!DefaultJDBCWrapper.tableExists(conn, tableName)) {
        Thread.sleep(1000)
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      }
      val loadedDf = read.option("dbtable", tableName).load()
      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    }
  }
}

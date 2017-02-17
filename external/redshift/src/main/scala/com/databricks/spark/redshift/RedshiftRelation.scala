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

import scala.reflect.ClassTag

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.databricks.spark.redshift.Parameters.MergedParameters
import com.databricks.spark.redshift.Utils.escapeJdbcString
import com.databricks.spark.redshift.pushdown.FilterPushdown
import org.json4s.{DefaultFormats, JValue, StreamInput}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val creds = AWSCredentialsUtils.load(
    params, sqlContext.sparkContext.hadoopConfiguration)

  if (sqlContext != null) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
    Utils.injectRedshiftPushdownRule(sqlContext.sparkSession)
  }

  private val tableNameOrSubquery =
    params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val tableNameOrSubquery =
        params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.resolveTable(conn, tableNameOrSubquery)
      } finally {
        conn.close()
      }
    }
  }

  override def toString: String =
    if (params.table.isDefined || tableNameOrSubquery.length() < 55) {
      s"RedshiftRelation($tableNameOrSubquery)"
    } else {
      val start = tableNameOrSubquery.substring(0, 25)
      val end = tableNameOrSubquery.substring(tableNameOrSubquery.length()-25)
      s"RedshiftRelation($start [...] $end} )"
    }


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val saveMode = if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val writer = new RedshiftWriter(jdbcWrapper, s3ClientFactory)
    writer.saveToRedshift(sqlContext, data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  /** The textual UNLOAD command exactly as sent to Redshift via JDBC. */
  def unloadStatement: Option[String] = _unloadStatement
  private var _unloadStatement: Option[String] = None

  /** The (unescaped) Select SQL statement sent to Redshift within the UNLOAD command. */
  def selectStatement: Option[String] = _selectStatement
  private var _selectStatement: Option[String] = None

  /**
   * Build RDD result from PrunedFilteredScan interface. Maintain this here for backwards
   * compatibility and for when extra pushdown are disabled.
   */
  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    Utils.checkThatRedshiftAndS3RegionsAreTheSame(
      params.jdbcUrl, params.rootTempDir, s3ClientFactory(creds))
    Utils.checkThatBucketHasObjectLifecycleConfiguration(
      params.rootTempDir, s3ClientFactory(creds))

    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      log.info(
      s"""Executing count(*) query:
         |$countQuery}
      """.stripMargin)
      _selectStatement = Some(countQuery)

      val conn = jdbcWrapper.getConnector(params)
      try {
        val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(countQuery))
        if (results.next()) {
          val numRows = results.getLong(1)
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = RowEncoder(StructType(Seq.empty)).toRow(Row(Seq.empty))
          sqlContext.sparkContext
            .parallelize(1L to numRows, parallelism)
            .map(_ => emptyRow)
            .asInstanceOf[RDD[Row]]
        } else {
          throw new IllegalStateException("Could not read count from Redshift")
        }
      } finally {
        conn.close()
      }
    } else {
      // Unload data from Redshift into a temporary directory in S3:
      val tempDir = params.createPerQueryTempDir()
      val prunedSchema = pruneSchema(schema, requiredColumns)

      val (select, unload) = buildUnloadStatement(
        buildStandardQuery(requiredColumns, filters),
        params.singleOutput, tempDir, creds)

      _selectStatement = Some(select)
      _unloadStatement = Some(unload)

      getRDDFromS3[Row](unload, tempDir, prunedSchema)
    }
  }

  // Get an RDD from an unload statement. Provide result schema because
  // when a custom SQL statement is used, this means that we cannot know the results
  // without first executing it.
  private def getRDDFromS3[T: ClassTag](
      unloadStatement: String, tempDir: String,
      resultSchema: StructType): RDD[T] = {
    val conn = jdbcWrapper.getConnector(params)

    log.info(
      s"""Executing UNLOAD statement:
         |${Utils.sanitizeQueryText(unloadStatement)}
      """.stripMargin)

    try {
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadStatement))
    } finally {
      conn.close()
    }
    // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
    // We need to use a manifest in order to guard against S3's eventually-consistent listings.
    val filesToRead: Seq[String] = {
      val cleanedTempDirUri =
        Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
      val s3URI = Utils.createS3URI(cleanedTempDirUri)
      val s3Client = s3ClientFactory(creds)
      val is = s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
      val s3Files: Seq[String] = try {
        implicit val format = DefaultFormats
        val json: JValue = JsonMethods.parse(StreamInput(is))
        val entries = (json \ "entries").extract[Array[JValue]]
        entries.map(e => (e \ "url").extract[String])
      } finally {
        is.close()
      }
      // The filenames in the manifest are of the form s3://bucket/key, without credentials.
      // If the S3 credentials were originally specified in the tempdir's URI, then we need to
      // reintroduce them here
      s3Files.map { file =>
        tempDir.stripSuffix("/") + '/' + file.stripPrefix(cleanedTempDirUri).stripPrefix("/")
      }
    }

    sqlContext.read
      .format(classOf[RedshiftFileFormat].getName)
      .schema(resultSchema)
      .load(filesToRead: _*)
      .queryExecution.executedPlan.execute().asInstanceOf[RDD[T]]
  }


  override def needConversion: Boolean = false

  // Build a query out of required columns and filters. (Used by buildScan)
  private def buildStandardQuery(requiredColumns: Array[String], filters: Array[Filter]): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names, and uppercase-cast them to make them equivalent to being unquoted
    // (unless already quoted):
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    s"SELECT $columnList FROM $tableNameOrSubquery $whereClause"
  }

  private def buildUnloadStatement(
      query: String, singleOutput: Boolean, tempDir: String,
      creds: AWSCredentialsProvider): (String, String) = {
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)

    log.debug("Unescaped SQL query: \n\n" + Utils.prettyPrint(query) + "\n\n")

    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val parallelClause = if (singleOutput) "PARALLEL FALSE" else ""

    val unloadStatement = s"UNLOAD ('${escapeJdbcString(query)}') " +
      s"TO '$fixedUrl' " +
      s"WITH CREDENTIALS '$credsString' " +
      s"ESCAPE MANIFEST $parallelClause"

    (query, unloadStatement)
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}

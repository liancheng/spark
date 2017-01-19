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

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.databricks.spark.redshift.Parameters.MergedParameters
import com.databricks.spark.redshift.Utils.escapeJdbcString
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

  if (sqlContext != null) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
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

  override def toString: String = s"RedshiftRelation($tableNameOrSubquery)"

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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3ClientFactory(creds))
    ) {
      if (redshiftRegion != s3Region) {
        // We don't currently support `extraunloadoptions`, so even if Amazon _did_ add a `region`
        // option for this we wouldn't be able to pass in the new option. However, we choose to
        // err on the side of caution and don't throw an exception because we don't want to break
        // existing workloads in case the region detection logic is wrong.
        log.error("The Redshift cluster and S3 bucket are in different regions " +
          s"($redshiftRegion and $s3Region, respectively). Redshift's UNLOAD command requires " +
          s"that the Redshift cluster and Amazon S3 bucket be located in the same region, so " +
          s"this read will fail.")
      }
    }
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))
    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      log.info(countQuery)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
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
      val unloadSql = buildUnloadStmt(requiredColumns, filters, tempDir, creds)
      log.info(unloadSql)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
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

      val prunedSchema = pruneSchema(schema, requiredColumns)

      sqlContext.read
        .format(classOf[RedshiftFileFormat].getName)
        .schema(prunedSchema)
        .load(filesToRead: _*)
        .queryExecution.executedPlan.execute().asInstanceOf[RDD[Row]]
    }
  }

  override def needConversion: Boolean = false

  private def buildUnloadStmt(
      requiredColumns: Array[String],
      filters: Array[Filter],
      tempDir: String,
      creds: AWSCredentialsProvider): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      s"SELECT $columnList FROM ${escapeJdbcString(tableNameOrSubquery)} " +
        s"${escapeJdbcString(whereClause)}"
    }
    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE MANIFEST"
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}

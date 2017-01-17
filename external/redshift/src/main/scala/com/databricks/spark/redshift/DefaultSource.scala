/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import org.slf4j.LoggerFactory

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Redshift Source implementation for Spark SQL
 */
class DefaultSource(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client)
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Default constructor required by Data Source API
   */
  def this() = this(DefaultJDBCWrapper, awsCredentials => new AmazonS3Client(awsCredentials))

  /**
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL. Resolves the schema
   * using JDBC connection over provided URL, which must contain credentials.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(jdbcWrapper, s3ClientFactory, params, None)(sqlContext)
  }

  /**
   * Load a RedshiftRelation using user-provided schema, so no inference over JDBC will be used.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(jdbcWrapper, s3ClientFactory, params, Some(schema))(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation(
      sqlContext: SQLContext,
      saveMode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    val table = params.table.getOrElse {
      throw new IllegalArgumentException(
        "For save operations you must specify a Redshift table name with the 'dbtable' parameter")
    }

    def tableExists: Boolean = {
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.tableExists(conn, table.toString)
      } finally {
        conn.close()
      }
    }

    val (doSave, dropExisting) = saveMode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if (tableExists) {
          sys.error(s"Table $table already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if (tableExists) {
          log.info(s"Table $table already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if (doSave) {
      val updatedParams = parameters.updated("overwrite", dropExisting.toString)
      new RedshiftWriter(jdbcWrapper, s3ClientFactory).saveToRedshift(
        sqlContext, data, saveMode, Parameters.mergeParameters(updatedParams))
    }

    createRelation(sqlContext, parameters)
  }
}

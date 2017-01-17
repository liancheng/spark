/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

package object redshift {

  /**
   * Wrapper of SQLContext that provide `redshiftFile` method.
   */
  implicit class RedshiftContext(sqlContext: SQLContext) {

    /**
     * Read a file unloaded from Redshift into a DataFrame.
     * @param path input path
     * @return a DataFrame with all string columns
     */
    def redshiftFile(path: String, columns: Seq[String]): DataFrame = {
      val sc = sqlContext.sparkContext
      val rdd = sc.newAPIHadoopFile(path, classOf[RedshiftInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
      // TODO: allow setting NULL string.
      val nullable = rdd.values.map(_.map(f => if (f.isEmpty) null else f)).map(x => Row(x: _*))
      val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))
      sqlContext.createDataFrame(nullable, schema)
    }

    /**
     * Reads a table unload from Redshift with its schema.
     */
    def redshiftFile(path: String, schema: StructType): DataFrame = {
      val casts = schema.fields.map { field =>
        col(field.name).cast(field.dataType).as(field.name)
      }
      redshiftFile(path, schema.fieldNames).select(casts: _*)
    }
  }
}

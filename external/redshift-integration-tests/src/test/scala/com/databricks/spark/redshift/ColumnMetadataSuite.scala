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
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * End-to-end tests of features which depend on per-column metadata (such as comments, maxlength).
 */
@ExtendedRedshiftTest
class ColumnMetadataSuite extends IntegrationSuiteBase {

  test("configuring maxlength on string columns") {
    withTempRedshiftTable("configuring_maxlength_on_string_column") { tableName =>
      val metadata = new MetadataBuilder().putLong("maxlength", 512).build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))), schema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 512)))
      // This append should fail due to the string being longer than the maxlength
      intercept[SQLException] {
        write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 513))), schema))
          .option("dbtable", tableName)
          .mode(SaveMode.Append)
          .save()
      }
    }
  }

  test("configuring compression on columns") {
    withTempRedshiftTable("configuring_compression_on_columns") { tableName =>
      val metadata = new MetadataBuilder().putString("encoding", "LZO").build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 128))), schema))
        .option("dbtable", s"PUBLIC.$tableName")
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 128)))
      val encodingDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable",
          s"""(SELECT "column", lower(encoding) FROM pg_table_def WHERE tablename='$tableName')""")
        .load()
      checkAnswer(encodingDF, Seq(Row("x", "lzo")))
    }
  }

  test("configuring comments on columns") {
    withTempRedshiftTable("configuring_comments_on_columns") { tableName =>
      val metadata = new MetadataBuilder().putString("description", "Hello Column").build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 128))), schema))
        .option("dbtable", s"PUBLIC.$tableName")
        .option("description", "Hello Table")
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 128)))
      val tableDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", s"(SELECT pg_catalog.obj_description('$tableName'::regclass))")
        .load()
      checkAnswer(tableDF, Seq(Row("Hello Table")))
      val commentQuery =
        s"""
           |(SELECT c.column_name, pgd.description
           |FROM pg_catalog.pg_statio_all_tables st
           |INNER JOIN pg_catalog.pg_description pgd
           |   ON (pgd.objoid=st.relid)
           |INNER JOIN information_schema.columns c
           |   ON (pgd.objsubid=c.ordinal_position AND c.table_name=st.relname)
           |WHERE c.table_name='$tableName')
         """.stripMargin
      val columnDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", commentQuery)
        .load()
      checkAnswer(columnDF, Seq(Row("x", "Hello Column")))
    }
  }
}

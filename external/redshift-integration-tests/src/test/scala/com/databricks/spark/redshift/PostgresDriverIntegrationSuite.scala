/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.tags.ExtendedRedshiftTest

/**
 * Basic integration tests with the Postgres JDBC driver.
 */
@ExtendedRedshiftTest
class PostgresDriverIntegrationSuite extends IntegrationSuiteBase {

  override def jdbcUrl: String = {
    super.jdbcUrl.replace("jdbc:redshift", "jdbc:postgresql")
  }

  test("postgresql driver takes precedence for jdbc:postgresql:// URIs") {
    val conn = DefaultJDBCWrapper.getConnector(None, jdbcUrl, None)
    try {
      // TODO(josh): this is slightly different than what was done in open-source spark-redshift.
      // This is due to conflicting PG driver being pulled in via transitive Spark test deps.
      // We should consider removing the postgres driver support entirely in our Databricks internal
      // version.
      assert(conn.getClass.getName === "org.postgresql.jdbc.PgConnection")
    } finally {
      conn.close()
    }
  }

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }
}

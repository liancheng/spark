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
 * This suite performs basic integration tests where the Redshift credentials have been
 * specified via `spark-redshift`'s configuration rather than as part of the JDBC URL.
 */
@ExtendedRedshiftTest
class RedshiftCredentialsInConfIntegrationSuite extends IntegrationSuiteBase {

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(df)
        .option("url", AWS_REDSHIFT_JDBC_URL)
        .option("user", AWS_REDSHIFT_USER)
        .option("password", AWS_REDSHIFT_PASSWORD)
        .option("dbtable", tableName)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = read
        .option("url", AWS_REDSHIFT_JDBC_URL)
        .option("user", AWS_REDSHIFT_USER)
        .option("password", AWS_REDSHIFT_PASSWORD)
        .option("dbtable", tableName)
        .load()
      assert(loadedDf.schema === df.schema)
      checkAnswer(loadedDf, df.collect())
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

}

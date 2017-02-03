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

import org.apache.spark.tags.ExtendedRedshiftTest

@ExtendedRedshiftTest
class RedshiftSSLIntegrationSuite extends IntegrationSuiteBase {

  test("SSL is not auto-configured if SSL options are set in the JDBC URI") {
    withTempRedshiftTable("jdbc_url_ssl_options_take_precedence_2") { tableName =>
      // If the user specifies SSL options in the URL then this takes precedence.
      // In the following test, the user-specified options will not work because
      // the sslRootCert is not specified and the Amazon certificate is assumed to
      // not be in the system truststore. Therefore, we expect this write to fail:
      val e = intercept[SQLException] {
        write(sqlContext.range(10))
          .option("user", AWS_REDSHIFT_USER)
          .option("password", AWS_REDSHIFT_PASSWORD)
          .option("url",
            s"$AWS_REDSHIFT_JDBC_URL?&ssl=true&sslMode=verify-full")
          .option("dbtable", tableName)
          .save()
      }
      assert(e.getMessage.contains("General SSLEngine problem"))
    }
  }
}

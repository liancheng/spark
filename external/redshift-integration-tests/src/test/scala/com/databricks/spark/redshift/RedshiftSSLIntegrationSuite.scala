/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.SQLException

import org.apache.commons.io.IOUtils

import org.apache.spark.tags.ExtendedRedshiftTest

@ExtendedRedshiftTest
class RedshiftSSLIntegrationSuite extends IntegrationSuiteBase {

  def testThatItWorks(url: String, autoSslOption: Option[Boolean]): Unit = {
    val dfReader = read
      .option("url", url)
      .option("user", AWS_REDSHIFT_USER)
      .option("password", AWS_REDSHIFT_PASSWORD)
      .option("query", "select 42")

    val finalDfReader = autoSslOption match {
      case Some(flag) => dfReader.option("autoenablessl", flag.toString)
      case _ => dfReader
    }
    val ret = finalDfReader.load().collect()
    assert(ret.size == 1 && ret(0)(0) == 42)
  }

  def testThatItFails(url: String, autoSslOption: Option[Boolean], errorMessage: String): Unit = {
    val e = intercept[SQLException] {
      testThatItWorks(url, autoSslOption)
    }
    assert(e.getMessage.contains(errorMessage))
  }

  val generalSslproblem = "General SSLEngine problem"
  val errorImportingCertificate = "Error importing certificate in to truststore."

  test("Simple test") {
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, None)
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, Some(false))
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, Some(true))
  }

  test("SSL is not auto-configured if SSL options are set in the JDBC URI") {
    // If the user specifies SSL options in the URL then this takes precedence.
    // In the following test, the user-specified options will not work because
    // the sslRootCert is not specified and the Amazon certificate is assumed to
    // not be in the system truststore. Therefore, we expect this write to fail:
    val url = s"$AWS_REDSHIFT_JDBC_URL?ssl=true&sslMode=verify-full"

    testThatItFails(url, None, generalSslproblem)
    testThatItFails(url, Some(true), generalSslproblem)
    testThatItFails(url, Some(false), generalSslproblem)
  }

  test("Make sure feature flag works in case of issues") {
    // Simulate a problem/bug that's related to this feature by
    // deleting the server certificate file.
    val existingCert = DefaultJDBCWrapper.redshiftSslCert
    assert(existingCert.exists())
    val certBackup = File.createTempFile("redshift-ssl-ca-cert", ".bkp")
    IOUtils.copy(new FileInputStream(existingCert), new FileOutputStream(certBackup))
    existingCert.delete()

    testThatItFails(AWS_REDSHIFT_JDBC_URL, None, errorImportingCertificate)
    testThatItFails(AWS_REDSHIFT_JDBC_URL, Some(true), errorImportingCertificate)

    // Disabling the feature should work around the issue and allow the user to run their query.
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, Some(false))

    // Restore the file and check that it all works again.
    IOUtils.copy(new FileInputStream(certBackup), new FileOutputStream(existingCert))
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, None)
    testThatItWorks(AWS_REDSHIFT_JDBC_URL, Some(true))
  }
}

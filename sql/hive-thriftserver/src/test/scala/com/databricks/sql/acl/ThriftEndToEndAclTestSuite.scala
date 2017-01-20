/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}

import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.thriftserver.{HiveThriftServer2Test, ServerMode}
import org.apache.spark.sql.jdbc.JdbcDialects

// scalastyle:off
/**
 * End-to-end test cases for SQL ACL queries. This test submits SQL queries for different users
 * using the thrift HTTP JDBC connection. This test is designed to simulate the way SQL ACLs will
 * be used in Databricks. Please note that:
 * - User tokens are propagated using HTTP headers.
 * - Spark is started using the same configuration (except the backend) that should be used in
 *   Databricks.
 *
 * Each case is loaded from a file in "spark/sql/hive-thriftserver/src/test/resources/acl-tests/inputs".
 * Each case has a golden result file in "spark/sql/hive-thriftserver/src/test/resources/acl-tests/results".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt -Phive -Phive-thriftserver "hive-thriftserver/test-only *ThriftEndToEndAclTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   build/sbt -Phive -Phive-thriftserver  "~thriftserver/test-only *ThriftEndToEndAclTestSuite -- -z base.sql"
 * }}}
 *
 * To re-generate golden files, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt -Phive -Phive-thriftserver "hive-thriftserver/test-only *ThriftEndToEndAclTestSuite"
 * }}}
 *
 * The format for input files is simple:
 *  1. A list of SQL queries separated by semicolon.
 *  2. Lines starting with @ change the current user.
 *  3. Lines starting with -- are treated as comments and ignored.
 *
 * For example:
 * {{{
 *   -- this is a comment
 *   select 1, -1;
 *   @usr1;
 *   select current_date;
 * }}}
 *
 * The format for golden result files look roughly like:
 * {{{
 *   -- some header information
 *
 *   -- !query 0
 *   select 1, -1
*   -- !query 0 token
 *   usr1
 *   -- !query 0 schema
 *   struct<...schema...>
 *   -- !query 0 output
 *   ... data row 1 ...
 *   ... data row 2 ...
 *   ...
 *
 *   -- !query 1
 *   ...
 * }}}
 *
 * TODO perhaps share sessions to see if that is working properly.
 */
// scalastyle:on
class ThriftEndToEndAclTestSuite extends HiveThriftServer2Test {
  private val dialect = JdbcDialects.get("jdbc:hive2")

  /**
   * We are only interested in HTTP mode.
   */
  override def mode: ServerMode.Value = ServerMode.http

  /**
   * Make sure the metastore uses the in-memory derby database.
   *
   * This saves ~5 seconds for each test.
   */
  override protected def metastoreJdbcUri: String =
    s"jdbc:derby:memory:;databaseName=$metastorePath;create=true"

  /**
   * Add the required the ACL configurations.
   */
  override protected def extraConf: Seq[String] = Seq(
    "--conf spark.thriftserver.customHeadersToProperties=" +
      "X-Databricks-User-Token:spark.databricks.token",
    "--conf spark.sql.extensions=" +
      "com.databricks.sql.acl.HiveAclExtensions",
    "--conf spark.databricks.acl.provider=" +
      "com.databricks.sql.acl.ReflectionBackedAclProvider",
    "--conf spark.databricks.acl.client=" +
      "com.databricks.sql.acl.SharedAclBackend",
    "--conf spark.databricks.acl.enabled=true"
  )

  /**
   * Create a JDBC connection for the given token. This connection will add the token value to
   * the `X-Databricks-User-Token` HTML header.
   */
  private def createConnection(token: String): Connection = {
    val user = System.getProperty("user.name")
    val uri = s"jdbc:hive2://localhost:$serverPort/default;" +
      "http.header.X-Databricks-User-Token=" + token + "?" +
      "hive.server2.transport.mode=http;" +
      "hive.server2.thrift.http.path=cliservice"
    DriverManager.getConnection(uri, user, "")
  }

  /**
   * Convert a row into a nice string. This code has been taken from Dataset.showString(...).
   */
  private def rowToString(row: Row): String = {
    val elements = row.toSeq.map {
      case null => "null"
      case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
      case array: Array[_] => array.mkString("[", ", ", "]")
      case seq: Seq[_] => seq.mkString("[", ", ", "]")
      case row: Row => rowToString(row)
      case cell => cell.toString
    }
    elements.mkString("[", ", ", "]")
  }

  private def cleanAnswer(answer: String): String = {
    // Get rid of the #1234 expression ids that show up in explain plans
    answer.replaceAll("#\\d+", "#x").trim
  }

  private def safeClose(closable: AutoCloseable): Unit = {
    try closable.close() catch {
      case NonFatal(_) =>
    }
  }

  /**
   * Execute the lines in a script.
   */
  private def execute(lines: Seq[(String, String)]): Seq[QueryOutput] = {
    val statements = mutable.Map.empty[String, Statement]
    try {
      lines.map {
        case (token, sql) =>
          // Open a connection.
          val statement = statements.getOrElseUpdate(
            token,
            createConnection(token).createStatement())

          var result: ResultSet = null
          try {
            // Execute the query.
            result = statement.executeQuery(sql)

            // Convert the result into something that we can easily compare.
            val schema = JdbcUtils.getSchema(result, dialect)
            val rows = JdbcUtils.resultSetToRows(result, schema).map(rowToString).toSeq.sorted
            QueryOutput(token, sql, schema.catalogString, cleanAnswer(rows.mkString("\n")))
          } catch {
            case e: SQLException =>
              QueryOutput(token, sql, "struct<>", cleanAnswer(e.getMessage))
          } finally {
            // Make sure the ResultSet gets closed.
            safeClose(result)
          }
      }
    } finally {
      // Clean-up created statements and connections.
      statements.values.foreach { statement =>
        val connection = statement.getConnection
        safeClose(statement)
        safeClose(connection)
      }
    }
  }

  /** ********************************************************
   * The code below has been taken from the SQLQueryTestSuite
   ** ******************************************************** */

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    if (regenerateGoldenFiles) {
      java.nio.file.Paths.get("src", "test", "resources", "acl-tests").toFile
    } else {
      val res = getClass.getClassLoader.getResource("acl-tests")
      new File(res.getFile)
    }
  }

  private val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  private val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  private def listTestCases(): Seq[TestCase] = {
    // Returns all the files (not directories) in a directory, recursively.
    def listFilesRecursively(path: File): Seq[File] = {
      val (dirs, files) = path.listFiles().partition(_.isDirectory)
      files ++ dirs.flatMap(listFilesRecursively)
    }
    listFilesRecursively(new File(inputFilePath)).map { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      TestCase(file.getName, file.getAbsolutePath, resultFile)
    }
  }

  /** List of test cases to ignore, in lower cases. */
  private val blackList = Set(
    "blacklist.sql"  // Do NOT remove this one. It is here to test the blacklist functionality.
  )

  // Create all the test cases.
  listTestCases().foreach(createScalaTestCase)

  /** A test case. */
  private case class TestCase(name: String, inputFile: String, resultFile: String)

  /** A single SQL query's output. */
  private case class QueryOutput(token: String, sql: String, schema: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex token\n" +
        token + "\n" +
        s"-- !query $queryIndex schema\n" +
        schema + "\n" +
        s"-- !query $queryIndex output\n" +
        output
    }
  }

  private def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.contains(testCase.name.toLowerCase)) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      test(testCase.name) { runTest(testCase) }
    }
  }

  /** Run a test case. */
  private def runTest(testCase: TestCase): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    // List of SQL queries to run
    val queries: Seq[(String, String)] = {
      val cleaned = input.split("\n").filterNot(_.startsWith("--")).mkString("\n")
      // note: this is not a robust way to split queries using semicolon, but works for now.
      val statements = cleaned.split("(?<=[^\\\\]);").map(_.trim).filter(_ != "")

      // Construct the principal-sql pairs. A developer can define for which principal a series of
      // sql statements is executed by adding a line with an @ with the user name (ended with a
      // semi colon) before the series.
      var token = "super"
      val queries = mutable.Buffer.empty[(String, String)]
      statements.foreach {
        case t if t.startsWith("@") =>
          token = t.substring(1)
        case sql =>
          queries += token -> sql
      }
      queries
    }

    // Run the queries.
    val outputs: Seq[QueryOutput] = execute(queries)

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          s"-- Number of queries: ${outputs.size}\n\n\n" +
          outputs.zipWithIndex.map{case (qr, i) => qr.toString(i)}.mkString("\n\n\n") + "\n"
      }
      stringToFile(new File(testCase.resultFile), goldenOutput)
    }

    // Read back the golden file.
    val expectedOutputs: Seq[QueryOutput] = {
      val goldenOutput = fileToString(new File(testCase.resultFile))
      val segments = goldenOutput.split("-- !query.+\n")

      // each query has 4 segments, plus the header
      assert(segments.size == outputs.size * 4 + 1,
        s"Expected ${outputs.size * 4 + 1} blocks in result file but got ${segments.size}. " +
          s"Try regenerate the result files.")
      Seq.tabulate(outputs.size) { i =>
        QueryOutput(
          sql = segments(i * 4 + 1).trim,
          token = segments(i * 4 + 2).trim,
          schema = segments(i * 4 + 3).trim,
          output = segments(i * 4 + 4).trim)
      }
    }

    // Compare results.
    assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
      outputs.size
    }

    outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
      assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
        output.sql
      }
      assertResult(expected.token, s"Token did not match for query #$i\n${expected.token}") {
        output.token
      }
      assertResult(expected.schema, s"Schema did not match for query #$i\n${expected.sql}") {
        output.schema
      }
      assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
        output.output
      }
    }
  }
}


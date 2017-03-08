/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.parser

import com.databricks.sql.transaction.directory.VacuumTableCommand

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class DatabricksSqlCommandParserSuite extends PlanTest {

  val parser = new DatabricksSqlParser(None, CatalystSqlParser)

  def checkAnswer(query: String, plan: LogicalPlan): Unit = {
    comparePlans(parser.parsePlan(query), plan)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parser.parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.toLowerCase.contains(message.toLowerCase))
    }
  }

  val testTbl1 = TableIdentifier("tbl1", Option("test"))
  val tbl1 = TableIdentifier("tbl1")
  val testPath = "/test/path"


  test("vacuum") {
    checkAnswer(
      s"VACUUM '$testPath'",
      VacuumTableCommand(Some(testPath), None, None))

    checkAnswer(
      s"""VACUUM "$testPath" RETAIN 1.25 HOURS""",
      VacuumTableCommand(Some(testPath), None, Some(1.25)))

    checkAnswer(
      "VACUUM `tbl1`",
      VacuumTableCommand(None, Some(tbl1), None))

    checkAnswer(
      "VACUUM test.tbl1 RETAIN 2 HOURS",
      VacuumTableCommand(None, Some(testTbl1), Some(2.0)))

    intercept("VACUUM")

    intercept("VACUUM TABLE `tbl1`")

    intercept(s"VACUUM $testPath") // because the given path is unquoted
  }

  test("Acl commands should not be parsed.") {
    intercept("GRANT SELECT ON TABLE test.tbl1 TO user1")
    intercept("REVOKE SELECT ON TABLE test.tbl1 FROM user1")
    intercept("SHOW GRANT eve ON x")
    intercept("ALTER TABLE test.tbl1 OWNER TO user1")
    intercept("MSCK REPAIR TABLE test.tbl1 PRIVILEGES")
  }

  test("pass through") {
    checkAnswer("select * from a union all select * from b",
      table("a").select(star()).union(table("b").select(star())))
    checkAnswer("select a, b from db.c where x < 1",
      table("db", "c").where('x < 1).select('a, 'b))
  }
}

/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import com.databricks.sql.acl.Action._
import com.databricks.sql.parser.DatabricksSqlParser
import com.databricks.sql.transaction.VacuumTableCommand

import org.apache.spark.sql.catalyst.{FunctionIdentifier, SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class AclCommandParserSuite extends PlanTest {

  val client = NoOpAclClient
  val catalog = new SessionCatalog(
    new InMemoryCatalog(),
    FunctionRegistry.builtin,
    SimpleCatalystConf(caseSensitiveAnalysis = false))

  val parser = new DatabricksSqlParser(Some(client), CatalystSqlParser)

  def checkAnswer(query: String, plan: LogicalPlan): Unit = {
    comparePlans(parser.parsePlan(query), plan)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parser.parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.toLowerCase.contains(message.toLowerCase))
    }
  }

  val testDb1 = Database("db1")
  val testTbl1 = Table(TableIdentifier("tbl1", Option("test")))
  val tbl1 = Table(TableIdentifier("tbl1"))
  val tblCatalog = Table(TableIdentifier("catalog"))
  val testPath = "/test/path"

  val functionReflect = Function(FunctionIdentifier("reflect"))

  val user1 = NamedPrincipal("user1")
  val user2 = NamedPrincipal("user2")
  val user3 = NamedPrincipal("user3")


  test("grant") {
    checkAnswer(
      "GRANT SELECT ON TABLE test.tbl1 TO user1",
      GrantPermissionsCommand(client, Permission(user1, Select, testTbl1) :: Nil))

    checkAnswer(
      "GRANT SELECT ON VIEW test.tbl1 TO user1",
      GrantPermissionsCommand(client, Permission(user1, Select, testTbl1) :: Nil))

    checkAnswer(
      "GRANT ALL PRIVILEGES ON CATALOG TO user2",
      GrantPermissionsCommand(client, Action.allRegularActions.map { action =>
        Permission(user2, action, Catalog)
      }))

    checkAnswer(
      "GRANT SELECT ON `catalog` TO user1",
      GrantPermissionsCommand(client, Permission(user1, Select, tblCatalog) :: Nil))

    checkAnswer(
      "GRANT SELECT ON FUNCTION reflect TO user3",
      GrantPermissionsCommand(client, Permission(user3, Select, functionReflect) :: Nil))

    checkAnswer(
      "GRANT SELECT ON ANONYMOUS FUNCTION TO user3",
      GrantPermissionsCommand(
        client,
        Permission(user3, Select, AnonymousFunction) :: Nil))

    checkAnswer(
      "GRANT SELECT ON ANY FILE TO user3",
      GrantPermissionsCommand(
        client,
        Permission(user3, Select, AnyFile) :: Nil))

    intercept("GRANT BLOB ON x TO `r2d2`", "Unknown ActionType")

    intercept("GRANT SELECT ON CAR x TO `r2d2`")
  }

  test("with grant option unsupported") {
    intercept("GRANT SELECT, MODIFY ON tbl1 TO user1 WITH GRANT OPTION", "unsupported")
    intercept("REVOKE GRANT OPTION FOR SELECT, MODIFY ON tbl1 FROM user1", "unsupported")
  }

  test("revoke") {
    checkAnswer(
      "REVOKE SELECT ON TABLE test.tbl1 FROM user1",
      RevokePermissionsCommand(client, Permission(user1, Select, testTbl1) :: Nil))

    checkAnswer(
      "REVOKE SELECT ON VIEW test.tbl1 FROM user1",
      RevokePermissionsCommand(client, Permission(user1, Select, testTbl1) :: Nil))

    checkAnswer(
      "REVOKE ALL PRIVILEGES ON CATALOG FROM user2",
      RevokePermissionsCommand(client, Action.allRegularActions.map { action =>
        Permission(user2, action, Catalog)
      }))

    checkAnswer(
      "REVOKE SELECT ON FUNCTION reflect FROM user3",
      RevokePermissionsCommand(client, Permission(user3, Select, functionReflect) :: Nil))

    checkAnswer(
      "REVOKE SELECT ON ANONYMOUS FUNCTION FROM user3",
      RevokePermissionsCommand(
        client,
        Permission(user3, Select, AnonymousFunction) :: Nil))

    checkAnswer(
      "REVOKE SELECT ON ANY FILE FROM user3",
      RevokePermissionsCommand(
        client,
        Permission(user3, Select, AnyFile) :: Nil))

    intercept("GRANT MOVE ON x TO c3po", "Unknown ActionType")

    intercept("GRANT SELECT ON TAXI x TO c3po")
  }

  test("show grant") {
    intercept("SHOW GRANT ON ALL")

    checkAnswer(
      "SHOW GRANT eve ON x",
      ShowPermissionsCommand(
        client,
        Option(NamedPrincipal("eve")),
        Table(TableIdentifier("x"))))

    checkAnswer(
      "SHOW GRANT ON DATABASE db1",
      ShowPermissionsCommand(client, None, testDb1))

    intercept("SHOW GRANT tim ON ALL")

    intercept("SHOW GRANT ON KEYBOARD bubba")
  }

  test("change owner") {
    checkAnswer(
      "ALTER TABLE test.tbl1 OWNER TO user1",
      ChangeOwnerCommand(client, testTbl1, user1))

    checkAnswer(
      "ALTER DATABASE db1 OWNER TO user2",
      ChangeOwnerCommand(client, testDb1, user2))

    checkAnswer(
      "ALTER CATALOG OWNER TO user3",
      ChangeOwnerCommand(client, Catalog, user3))

    intercept("ALTER KEYBOARD bubba OWNER TO ave")
  }

  test("repair dangling acls") {
    checkAnswer(
      "MSCK REPAIR TABLE test.tbl1 PRIVILEGES",
      CleanPermissionsCommand(client, testTbl1))

    checkAnswer(
      "MSCK REPAIR DATABASE db1 PRIVILEGES",
      CleanPermissionsCommand(client, testDb1))

    intercept("MSCK REPAIR KEYBOARD bubba PRIVILEGES")
  }

  test("vacuum should still work") {
    checkAnswer(
      s"VACUUM '$testPath'",
      VacuumTableCommand(Some(testPath), None, None))

    checkAnswer(
      s"""VACUUM "$testPath" RETAIN 1.25 HOURS""",
      VacuumTableCommand(Some(testPath), None, Some(1.25)))

    checkAnswer(
      "VACUUM `tbl1`",
      VacuumTableCommand(None, Some(tbl1.key), None))

    checkAnswer(
      "VACUUM test.tbl1 RETAIN 2 HOURS",
      VacuumTableCommand(None, Some(testTbl1.key), Some(2.0)))

    intercept("VACUUM")

    intercept("VACUUM TABLE `tbl1`")

    intercept(s"VACUUM $testPath") // because the given path is unquoted
  }

  test("pass through") {
    checkAnswer("select * from a union all select * from b",
      table("a").select(star()).union(table("b").select(star())))
    checkAnswer("select a, b from db.c where x < 1",
      table("db", "c").where('x < 1).select('a, 'b))
  }

  test("test non-reserved identifiers") {
    val nonReserved = Seq("ALTER", "OWNER", "TO", "MSCK", "REPAIR", "PRIVILEGES", "SHOW", "GRANT",
      "ON", "ALL", "WITH", "OPTION", "REVOKE", "FOR", "FROM", "CATALOG", "DATABASE", "TABLE",
      "VIEW", "FUNCTION", "ANONYMOUS", "FILE")
    nonReserved.foreach { keyword =>
      def testShowGrant(securable: Securable): Unit = {
        checkAnswer(
          s"SHOW GRANT ON $keyword",
          ShowPermissionsCommand(client, None, securable))
      }
      keyword match {
        case "ALL" => intercept("SHOW GRANT ON ALL")
        case "CATALOG" => testShowGrant(Catalog)
        case name => testShowGrant(Table(TableIdentifier(name)))
      }
    }
  }
}

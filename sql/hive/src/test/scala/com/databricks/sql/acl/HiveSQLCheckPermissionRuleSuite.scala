/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import java.nio.file.Files

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.hive.execution.UDFIntegerToString
import org.apache.spark.sql.hive.test.TestHiveExtensions

object TestAclClient extends InMemoryAclClient(NamedPrincipal("U")) {
  setOwner(Catalog, NamedPrincipal("super"))
  def getOwner(securable: Securable): Option[Principal] = {
    getOwners(Seq(securable)).get(securable)
  }

  override def getValidPermissions(requests: Seq[(Securable, Action)])
      : Set[(Securable, Action)] = {
    if (underlyingPrincipal == NamedPrincipal("super")) {
      requests.toSet
    } else {
      super.getValidPermissions(requests)
    }
  }
}

object TestHiveAclExtensions extends HiveAclExtensions {
  override def client(session: SparkSession): AclClient = TestAclClient
}

class HiveSQLCheckPermissionRuleSuite extends TestHiveExtensions(TestHiveAclExtensions)
    with BeforeAndAfterEach {
  override def beforeAll(): Unit = {
    super.beforeAll
    SparkSession.setActiveSession(spark)
    asUser("super") { spark.sql(s"create database perm") }
  }

  override def afterAll(): Unit = {
    try {
      TestAclClient.underlyingPrincipal = NamedPrincipal("super")
      spark.sql("use default")
      spark.sql("drop database perm cascade")
    } finally {
      super.afterAll()
    }
  }

  override def afterEach(): Unit = {
    TestAclClient.clearAll()
    spark.catalog.setCurrentDatabase("default")
  }

  override def beforeEach(): Unit = {
    TestAclClient.underlyingPrincipal = NamedPrincipal("U")
  }

  def withDatabase(dbName: String, owner: String)(action: => Unit): Unit = {
    try {
      asUser(owner) { spark.sql(s"create database $dbName") }
      action
    } finally {
      asUser("super") {
        if (spark.catalog.databaseExists(dbName)) {
          spark.sql(s"drop database $dbName cascade")
        }
      }
    }
  }

  def withTable(tableName: TableIdentifier, owner: String, isDataSource: Boolean = true)
      (action: => Unit): Unit = {
    val createSql = if (isDataSource) {
      s"create table ${tableName.toString} (a int, p int)"
    } else {
      s"create table ${tableName.toString} (a int) partitioned by (p int)"
    }

    try {
      asUser(owner) { spark.sql(createSql) }
      action
    } finally {
      asUser("super") {
        spark.sql(s"drop table if exists $tableName")
      }
    }
  }

  def withView(
      viewName: TableIdentifier,
      sourceName: TableIdentifier,
      owner: String)(
      action: => Unit): Unit = {
    try {
      val temporary = if (viewName.database.isDefined) "" else "temporary"
      asUser(owner) {
        spark.sql(s"create $temporary view $viewName as select * from $sourceName")
      }
      action
    } finally {
      asUser("super") {
        spark.sql(s"drop view if exists $viewName")
      }
    }
  }

  def asUser(userName: String)(action: => Unit): Unit = {
    val prevUser = TestAclClient.underlyingPrincipal
    try {
      TestAclClient.underlyingPrincipal = NamedPrincipal(userName)
      action
    } finally {
      TestAclClient.underlyingPrincipal = prevUser
    }
  }

  test("create database") {
    intercept[SecurityException] {
      withDatabase("u1", "U") {}
    }
    try {
      asUser("super") { spark.sql("grant create on catalog to U") }
      withDatabase("u1", "U") {
        assert(TestAclClient.getOwner(Database("u1")).orNull === NamedPrincipal("U"))
      }
    } finally {
      asUser("super") { spark.sql("revoke create on catalog from U") }
    }
  }

  test("drop database") {
    withDatabase("test", "super") {
      intercept[SecurityException] {
        asUser("U") { spark.sql("drop database test") }
      }
      asUser("super") { spark.sql("alter database test owner to U") }
      asUser("U") { spark.sql("drop database test") }
    }
  }

  test("create table") {
    val tableId = TableIdentifier("t1", Some("perm"))
    intercept[SecurityException] {
      withTable(tableId, "U") {}
    }
    asUser("super") { spark.sql("grant create on database perm to U") }
    withTable(tableId, "U") {
      assert(TestAclClient.getOwner(Table(tableId)).orNull === NamedPrincipal("U"))
    }
    asUser("super") { spark.sql("revoke create on database perm from U") }
  }

  test("drop table") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      intercept[SecurityException] {
        asUser("U") { spark.sql(s"drop table $tableId") }
      }
      asUser("super") { spark.sql(s"alter table $tableId owner to U") }
      asUser("U") { spark.sql(s"drop table $tableId") }
    }
  }

  test("select from table") {
    val tableId = TableIdentifier("t7", Some("perm"))
    withTable(tableId, "super") {
      asUser("super") { spark.sql(s"SELECT * FROM $tableId") }
      intercept[SecurityException] {
        asUser("U") { spark.sql(s"SELECT * FROM $tableId") }
      }
      asUser("super") { spark.sql(s"grant select on table $tableId to U") }
      asUser("U") { spark.sql(s"SELECT * FROM $tableId") }
    }
  }

  test("create temp view") {
    /**
     * Cannot create perm tables but can create temp tables
     */
    intercept[SecurityException] {
      withTable(TableIdentifier("t1", Some("perm")), "U") {}
    }
    try {
      asUser("U") { spark.sql(s"create temporary view v1 as select 1") }
    } finally {
      asUser("U") { spark.sql("drop view v1") }
    }
  }

  test("select from temp view") {
    val tableId = TableIdentifier("t1", Some("perm"))
    val viewId = TableIdentifier("v1", None)
    withTable(tableId, "super") {
      withView(viewId, tableId, "super") {
        intercept[SecurityException] {
          asUser("U") { spark.sql("select * from v1") }
        }
        asUser("super") { spark.sql(s"grant select on table $tableId to U") }
        asUser("U") { spark.sql("select * from v1") }
      }
    }
  }

  test("create and select from perm view") {
    val tableId = TableIdentifier("t1", Some("perm"))
    val viewId = TableIdentifier("v1", Some("perm"))
    withTable(tableId, "super") {
      withView(viewId, tableId, "super") {
        intercept[SecurityException] {
          asUser("U") { spark.sql(s"select * from $viewId") }
        }
        asUser("super") { spark.sql(s"grant select on table $tableId to U") }
        intercept[SecurityException] {
          asUser("U") { spark.sql(s"select * from $viewId") }
        }
        asUser("super") { spark.sql(s"revoke select on table $tableId from U") }
        asUser("super") { spark.sql(s"grant select on view $viewId to U") }
        asUser("U") { spark.sql(s"select * from $viewId") }
        asUser("super") { spark.sql(s"revoke select on view $viewId from U") }
      }
    }
  }

  test("Caching is irrelevant") {
    val t1 = TableIdentifier("t1", Some("perm"))
    val t2 = TableIdentifier("t2", Some("perm"))
    val joinSql = s"select * from $t1 cross join (select a from $t2 where p < 5)"
    withTable(t1, "super") {
      asUser("super") { spark.sql(s"insert into $t1 select * from " +
        s"range(1, 10) cross join range(1, 10)").collect() }
      withTable(t2, "super") {
        asUser("super") { spark.sql(s"insert into $t2 select * from " +
          s"range(1, 10) cross join range(1, 10)").collect() }
        asUser("super") { spark.sql(s"grant select on $t2 to U") }
        intercept[SecurityException] {
          asUser("U") { spark.sql(joinSql) }
        }
        asUser("U") { spark.sql(s"select a from $t2 where p < 5").cache() }
        asUser("super") {
          assert(spark.sql(joinSql).queryExecution.optimizedPlan
            .find(_.isInstanceOf[InMemoryRelation]).isDefined)
        }
        intercept[SecurityException] {
          asUser("U") { spark.sql(joinSql) }
        }
        asUser("super") { spark.sql(s"grant select on $t1 to U") }
        asUser("U") { spark.sql(joinSql) }
      }
    }
  }

  test("temporary function") {
    asUser("U") {
      spark.sql(s"create temporary function testUDF AS '${classOf[UDFIntegerToString].getName}'")
    }
    intercept[SecurityException] {
      asUser("U") {
        spark.sql(s"select testUDF(id) from (select 1 as id) A")
      }
    }
    try {
      asUser("super") {
        spark.sql("grant select on anonymous function to U")
      }
      asUser("U") {
        spark.sql(s"select testUDF(id) from (select 1 as id) A")
      }
    } finally {
      asUser("super") {
        spark.sql("drop temporary function testUDF")
      }
    }
  }

  test("permanent function") {
    intercept[SecurityException] {
      asUser("U") {
        spark.sql(s"create function perm.`test__UDF` " +
          s"AS '${classOf[UDFIntegerToString].getName}'")
      }
    }

    try {
      asUser("super") { spark.sql("grant create_named_function on database perm to U") }
      asUser("U") {
        spark.sql("use perm")
        spark.sql(s"create function `test__UDF` " +
          s"AS '${classOf[UDFIntegerToString].getName}'")
      }
    } finally {
      asUser("super") { spark.sql("revoke create_named_function on database perm from U")}
    }

    asUser("U") {
      spark.sql("use perm")
      spark.sql(s"select `test__UDF`(id) from (select 1 as id) A")
    }

    intercept[SecurityException] {
      asUser("W") {
        spark.sql("use perm")
        spark.sql(s"select `test__UDF`(id) from (select 1 as id) A")
      }
    }

    try {
      asUser("U") {
        spark.sql("grant select on function perm.`test__UDF` to W")
      }
      asUser("W") {
        spark.sql(s"select `test__UDF`(id) from (select 1 as id) A")
      }
    } finally {
      asUser("U") {
        spark.sql("revoke select on function perm.`test__UDF` from W")
      }
      asUser("super") { spark.sql("drop function `test__UDF`") }
    }
  }

  test("alter table partition") {
    val tableId = TableIdentifier("t1", Some("perm"))
    val alterAddString = s"alter table $tableId add partition (p=10)"
    val alterDropString = s"alter table $tableId drop partition (p=11)"
    val alterRenameString = s"alter table $tableId partition (p=10) " +
      s"rename to partition (p=11)"
    val alters = Seq(alterAddString, alterRenameString, alterDropString)
    withTable(tableId, "super", isDataSource = false) {
      asUser("super") { alters.foreach(spark.sql) }
      asUser("U") {
        alters.foreach {alter =>
          intercept[SecurityException] {
            spark.sql(alter)
          }
        }
      }
      asUser("super") {
        spark.sql(s"grant select on table $tableId to U")
      }
      asUser("U") {
        alters.foreach {alter =>
          intercept[SecurityException] {
            spark.sql(alter)
          }
        }
      }
      asUser("super") {
        spark.sql(s"grant modify on table $tableId to U")
      }
      asUser("U") { alters.foreach(spark.sql) }
    }
  }

  test("grant/revoke by owner only") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      asUser("super") { spark.sql(s"grant select on $tableId to U") }
      intercept[SecurityException] {
        asUser("W") {
          spark.sql(s"grant select on $tableId to U")
        }
      }
      asUser("super") { spark.sql(s"revoke select on $tableId from U") }
      intercept[SecurityException] {
        asUser("W") {
          spark.sql(s"revoke select on $tableId from U")
        }
      }
    }
  }

  test("grant perm does not imply with grant") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"select * from $tableId")
        }
      }
      asUser("super") { spark.sql(s"grant select on $tableId to U") }
      asUser("U") {
        spark.sql(s"select * from $tableId")
      }
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"grant select on $tableId to W")
        }
      }
    }
  }

  test("revoke works with or without grant") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      asUser("super") { spark.sql(s"revoke select on $tableId from U") }
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"select * from $tableId")
        }
      }
      asUser("super") { spark.sql(s"grant select on $tableId to U") }
      asUser("U") {
        spark.sql(s"select * from $tableId")
      }
      asUser("super") { spark.sql(s"revoke select on $tableId from U") }
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"select * from $tableId")
        }
      }
    }
  }

  ignore("grant with option allows delegate to perform action, grant and revoke") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"grant select on $tableId to W")
        }
      }
      asUser("super") { spark.sql(s"grant select on $tableId to U with grant option") }
      asUser("U") {
        spark.sql(s"select * from $tableId")
        spark.sql(s"grant select on $tableId to W")
      }
      asUser("W") { spark.sql(s"select * from $tableId") }
      asUser("U") {
        intercept[SecurityException] {
          spark.sql(s"grant select on $tableId to W with grant option")
        }
        intercept[SecurityException] {
          spark.sql(s"grant modify on $tableId to W")
        }
      }
      asUser("U") { spark.sql(s"revoke select on $tableId from W") }
      intercept[SecurityException] {
        asUser("W") { spark.sql(s"select * from $tableId") }
      }
      intercept[SecurityException] {
        asUser("U") { spark.sql(s"revoke grant option for select on $tableId from U") }
      }
    }
  }

  ignore("revoke with option allows delegate to perform action") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      asUser("super") {
        spark.sql(s"grant select on $tableId to U with grant option")
        spark.sql(s"revoke grant option for select on $tableId from U")
      }
      intercept[SecurityException] {
        asUser("U") {
          spark.sql(s"grant select on $tableId to W")
        }
      }
      asUser("U") {
        spark.sql(s"select * from $tableId")
      }
    }
  }

  ignore("revoke also removes with option") {
    val tableId = TableIdentifier("t1", Some("perm"))
    withTable(tableId, "super") {
      asUser("super") {
        spark.sql(s"grant select on $tableId to U with grant option")
        spark.sql(s"revoke select on $tableId from U")
      }
      asUser("U") {
        intercept[SecurityException] {
          spark.sql(s"grant select on $tableId to W")
        }
        intercept[SecurityException] {
          spark.sql(s"select * from $tableId")
        }
      }
    }
  }

  test("explain query") {
    asUser("U") {
      spark.sql(s"create temp function fn1 AS '${classOf[UDFIntegerToString].getName}'")
    }
    withTable(TableIdentifier("t1", Some("perm")), "super") {
      withTable(TableIdentifier("t2", Some("perm")), "super") {
        try {
          asUser("super") {
            spark.sql(
              "create or replace view perm.vw1 as select t1.a, t2.p from perm.t1, perm.t2")
          }
          asUser("U") {
            intercept[SecurityException](
              spark.sql("explain select fn1(a) from perm.t1"))
            intercept[SecurityException](
              spark.sql("select fn1(t1.a), t2.* from perm.t1, perm.t2"))
            intercept[SecurityException](
              spark.sql("explain select * from perm.vw1"))
          }
          asUser("super") {
            spark.sql("grant read_metadata on perm.vw1 to U")
          }
          asUser("U") {
            spark.sql("explain select * from perm.vw1")
          }
          asUser("super") {
            spark.sql("grant select on anonymous function to U")
            spark.sql("grant read_metadata on perm.t1 to U")
            spark.sql("grant select on perm.t1 to U")
            spark.sql("grant select on perm.t2 to U")
          }
          asUser("U") {
            spark.sql("explain select fn1(a) from perm.t1")
            spark.sql("select fn1(t1.a), t2.* from perm.t1, perm.t2")
            intercept[SecurityException](spark.sql(
              "explain select fn1(t1.a), t2.* from perm.t1, perm.t2"))
          }
          asUser("super") {
            spark.sql("grant read_metadata on perm.t2 to U")
          }
          asUser("U") {
            spark.sql("explain select fn1(t1.a), t2.* from perm.t1, perm.t2")
          }
        } finally {
          asUser("super") {
            spark.sql("drop view if exists perm.vw1")
            spark.sql("drop temp function fn1")
          }
        }
      }
    }
  }

  test("drop non existing objects") {
    def assertFail(sql: String): Unit = {
      intercept[AnalysisException](spark.sql(sql))
    }
    def assertNoOp(sql: String): Unit = {
      assert(spark.sql(sql).queryExecution.analyzed === NoOpRunnableCommand)
    }
    assertFail("drop database dbx")
    assertNoOp("drop database if exists dbx")
    assertFail("drop table perm.tblx")
    assertNoOp("drop table if exists perm.tblx")
    assertFail("drop function perm.fnx")
    assertNoOp("drop function if exists perm.fnx")
  }

  test("SC-5492: drop table using an action") {
    def showTables(): Seq[Row] = spark.sql("show tables").collect()
    withDatabase("test_sql_acl_db", "super") {
      asUser("super") {
        spark.sql("use test_sql_acl_db")
        spark.sql("drop table if exists mustdrop").collect()
        assert(showTables() === Seq())
        spark.sql("create table mustdrop(id bigint)")
        assert(showTables() === Seq(Row("test_sql_acl_db", "mustdrop", false)))
        spark.sql("drop table mustdrop").collect()
        assert(showTables() === Seq())
      }
    }
  }

  test("query on file") {
    val path = Files.createTempDirectory("somedata").toString + "/SC-5483"
    asUser("super") {
      spark.range(1000).write.parquet(path)
    }
    asUser("U") {
      intercept[SecurityException] {
        spark.sql(s"select * from parquet.`$path`")
      }
      intercept[SecurityException] {
        spark.sql(s"create temporary view x using parquet options(path '$path')")
      }
    }
    asUser("super") {
      spark.sql("grant select on any file to U")
    }
    asUser("U") {
      assert(spark.sql(s"select * from parquet.`$path`").count === 1000L)
      spark.sql(s"create temporary view x using parquet options(path '$path')")
      assert(spark.sql("select * from x").count === 1000L)
      spark.sql(s"drop view x")
    }
  }
}

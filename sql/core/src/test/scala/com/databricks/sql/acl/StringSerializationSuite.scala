/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}

/**
 * Test suite testing from and to String serialization for [[Securable]]s and [[Action]]s.
 */
class StringSerializationSuite extends SparkFunSuite{
  // Lets see if I've got a DB interview question right ...
  test("parse name parts") {
    def checkSimpleRoundTrip(in: String, out: String): Unit = {
      assert(out === Securable.addBackticks(in))
      assert(Seq(in) === Securable.parseString(out))
    }
    checkSimpleRoundTrip("a", "`a`")
    checkSimpleRoundTrip("a/ee", "`a/ee`")
    checkSimpleRoundTrip("a/``ee", "`a/````ee`")

    def checkParse(in: String, out: String*): Unit = {
      assert(Securable.parseString(in) === out)
    }

    checkParse("a/b/a", "a", "b", "a")
    checkParse("ae``rw/bree/bree", "ae``rw", "bree", "bree")
    checkParse("/ae``rw/`bree`/bree", "ae``rw", "bree", "bree")
    checkParse("/foo``b/`br//ee`/bree", "foo``b", "br//ee", "bree")
    checkParse("/foob//`br//ee`/bree", "foob", "", "br//ee", "bree")

    def checkFail(in: String, errors: String*): Unit = {
      val e = intercept[IllegalArgumentException](Securable.parseString(in))
      val msg = e.getMessage
      errors.foreach { error =>
        assert(msg.contains(error))
      }
    }

    checkFail("`nice`/`oops", "name part should be terminated by a backtick")
    checkFail("`nice`/`oo`ps", "should not have characters outside", "surrounding backticks")
  }

  def roundTripSecurable(in: Securable, out: String): Unit = {
    assert(in.name === out)
    assert(Securable.fromString(out) === in)
  }

  test("catalog") {
    roundTripSecurable(Catalog, "/CATALOG/`default`")
  }

  test("database") {
    roundTripSecurable(
      Database("db1"),
      "/CATALOG/`default`/DATABASE/`db1`")
    roundTripSecurable(
      Database("datuh```base"),
      "/CATALOG/`default`/DATABASE/`datuh``````base`")
  }

  test("table") {
    roundTripSecurable(
      Table(TableIdentifier("tbl1", Some("+1"))),
      "/CATALOG/`default`/DATABASE/`+1`/TABLE/`tbl1`")
    roundTripSecurable(
      Table(TableIdentifier("tbl1", Some("dbx"))),
      "/CATALOG/`default`/DATABASE/`dbx`/TABLE/`tbl1`")
    roundTripSecurable(
      Table(TableIdentifier("`tbl1`", Some("dbx"))),
      "/CATALOG/`default`/DATABASE/`dbx`/TABLE/```tbl1```")
    roundTripSecurable(
      Table(TableIdentifier("`tb/l1`", Some("d`bx"))),
      "/CATALOG/`default`/DATABASE/`d``bx`/TABLE/```tb/l1```")

    val e = intercept[IllegalStateException] {
      Table(TableIdentifier("tbl1", None)).name
    }
    assert(e.getMessage.contains("A database is not defined for TABLE"))
  }

  test("function") {
    roundTripSecurable(
      Function(FunctionIdentifier("tbl1", Some("super`sci-fi"))),
      "/CATALOG/`default`/DATABASE/`super``sci-fi`/FUNCTION/`tbl1`")
    roundTripSecurable(
      Function(FunctionIdentifier("fn1", Some("dbx"))),
      "/CATALOG/`default`/DATABASE/`dbx`/FUNCTION/`fn1`")
    roundTripSecurable(
      Function(FunctionIdentifier("`fn1`", Some("db__x"))),
      "/CATALOG/`default`/DATABASE/`db__x`/FUNCTION/```fn1```")
    roundTripSecurable(
      Function(FunctionIdentifier("`tb/l1`", Some("la`data`base"))),
      "/CATALOG/`default`/DATABASE/`la``data``base`/FUNCTION/```tb/l1```")

    val e = intercept[IllegalStateException] {
      Function(FunctionIdentifier("fn", None)).name
    }
    assert(e.getMessage.contains("A database is not defined for FUNCTION"))
  }

  test("anonymous objects") {
    roundTripSecurable(AnyFile, "/ANY_FILE")
    roundTripSecurable(AnonymousFunction, "/ANONYMOUS_FUNCTION")
  }

  test("invalid names") {
    def checkFail(in: String): Unit = {
      val e = intercept[IllegalArgumentException](Securable.fromString(in))
      assert(e.getMessage.contains("cannot be converted into a Securable"))
    }
    checkFail("CADALOG/`default`")
    checkFail("CATALOG/TABLE/qew")
    checkFail("CATALOG/default/TABLE/qew")
    checkFail("CATALOG/default/DATABASE/TABLE/qew")
    checkFail("//CATALOG/default/DATABASE/qew")
  }

  test("actions") {
    def roundTripAction(in: Action, out: String): Unit = {
      assert(in.name === out)
      assert(Action.fromString(out) === in)
      val grantIn = Action.Grant(in)
      val grantOut = s"GRANT_$out"
      assert(grantIn.name === grantOut)
      assert(Action.fromString(grantOut) === grantIn)
    }

    roundTripAction(Action.Select, "SELECT")
    roundTripAction(Action.ReadMetadata, "READ_METADATA")
    roundTripAction(Action.Modify, "MODIFY")
    roundTripAction(Action.Create, "CREATE")
    roundTripAction(Action.CreateNamedFunction, "CREATE_NAMED_FUNCTION")
    roundTripAction(Action.ModifyClasspath, "MODIFY_CLASSPATH")
    roundTripAction(Action.Own, "OWN")

    def toAction(in: String, out: Action): Unit = {
      assert(Action.fromString(in) === out)
      assert(Action.fromString(s"grAnT_$in") === Action.Grant(out))
    }
    toAction("SelEcT", Action.Select)
    toAction("Read_METAdaTa", Action.ReadMetadata)
    toAction("modifY", Action.Modify)
    toAction("CREATE_named_FUNCTION", Action.CreateNamedFunction)
    toAction("MoDiFY_CLaSSPaTH", Action.ModifyClasspath)
    toAction("own", Action.Own)

    def failAction(in: String): Unit = {
      intercept[IllegalArgumentException](Action.fromString(in))
      intercept[IllegalArgumentException](Action.fromString(s"GRANT_$in"))
    }
    failAction("selet")
    failAction("update")
    failAction("delete")
    failAction("truncate")
    failAction("viewdefinition")
  }
}

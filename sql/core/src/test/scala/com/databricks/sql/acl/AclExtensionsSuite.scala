/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

/**
 * Test the construction of a [[SparkSession]] with [[AclExtensions]].
 */
class AclExtensionsSuite extends SparkFunSuite with BeforeAndAfterEach {
  private def clearSession(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def beforeAll(): Unit = clearSession()

  override protected def afterEach(): Unit = clearSession()

  test("AclExtensions with Reflection ACL Backend") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions",
        classOf[AclExtensions].getCanonicalName)
      .config("spark.databricks.acl.provider",
        classOf[ReflectionBackedAclProvider].getCanonicalName)
      .config("spark.databricks.acl.client",
        classOf[AclClientBackend].getCanonicalName)
      .getOrCreate()

    try {
      session.udf.register("plusOne", (value: Int) => value + 1)
      session.sparkContext.setLocalProperty(TokenConf.TOKEN_KEY, "token")
      session.sql("select plusOne(id) from values 1,2,3,4,5 t(id)")
      assert(AclClientBackend.lastCommandArguments.nonEmpty)
    } finally {
      session.stop()
    }

  }

  test("AclExtensions with Reflection Dummy Backend") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions",
        classOf[AclExtensions].getCanonicalName)
      .config("spark.databricks.acl.provider",
        classOf[NoOpAclProvider].getCanonicalName)
      .getOrCreate()
    try {
      session.udf.register("plusOne", (value: Int) => value + 1)
      session.sql("select plusOne(id) from values 1,2,3,4,5 t(id)")
    } finally {
      session.stop()
    }
  }

  test("AclExtensions with bad conf") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[AclExtensions].getCanonicalName)
      .config("spark.databricks.acl.provider", "bad.AclProv")
      .getOrCreate()

    try {
      session.udf.register("plusOne", (value: Int) => value + 1)
      intercept[Error](session.sql("select plusOne(id) from values 1,2,3,4,5 t(id)"))
    } finally {
      session.stop()
    }
  }
}

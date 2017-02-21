/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.apache.spark.SparkFunSuite

class TableNameSuite extends SparkFunSuite {
  test("TableName.parseFromEscaped") {
    assert(TableName.parseFromEscaped("foo.bar") === TableName("foo", "bar"))
    assert(TableName.parseFromEscaped("foo") === TableName("foo"))
    assert(TableName.parseFromEscaped("\"foo\"") === TableName("foo"))
    assert(TableName.parseFromEscaped("\"\"\"foo\"\"\".bar") === TableName("\"foo\"", "bar"))
    // Dots (.) can also appear inside of valid identifiers.
    assert(TableName.parseFromEscaped("\"foo.bar\".baz") === TableName("foo.bar", "baz"))
    assert(TableName.parseFromEscaped("\"foo\"\".bar\".baz") === TableName("foo\".bar", "baz"))
  }

  test("TableName.toString") {
    assert(TableName("foo", "bar").toString === """"foo"."bar"""")
    assert(TableName("PUBLIC", "bar").toString === """"PUBLIC"."bar"""")
    assert(TableName("\"foo\"", "bar").toString === "\"\"\"foo\"\"\".\"bar\"")
    assert(TableName("bar").toString === "\"bar\"")
    assert(TableName("\"bar\"").toString === "\"\"\"bar\"\"\"")
  }
}

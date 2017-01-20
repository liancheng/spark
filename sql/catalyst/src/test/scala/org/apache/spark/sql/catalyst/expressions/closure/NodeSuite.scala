/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.catalyst.expressions.closure

import org.apache.xbean.asm5.Type.{BOOLEAN_TYPE, INT_TYPE}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.closure.DSL._

class NodeSuite extends SparkFunSuite {
  test("treeString") {
    val foo = FunctionCall(
      obj = cast[java.lang.String](arg[AnyRef]),
      className = "java.lang.String",
      method = "equals",
      arguments = List(Constant("lisa")),
      dataType = BOOLEAN_TYPE)

    assert(foo.treeString ==
      """FunctionCall[Z](java.lang.String, equals)
        |  Cast[Ljava/lang/String;]
        |    Argument[Ljava/lang/Object;]
        |  Constant[Ljava/lang/String;](lisa)
        |""".stripMargin)
    assert(foo.toString == "java.lang.String.equals(Cast(Argument,Ljava/lang/String;), lisa)")

    val ifElse = If(gt(arg[Int], Constant(3)), arg[Int], Constant(3), INT_TYPE)
    assert(ifElse.treeString ==
      """If[I](Argument > 3)
        |  Argument[I]
        |  Constant[I](3)
        |""".stripMargin)

    assert(ifElse.toString == "If(Argument > 3,Argument,3,I)")
  }
}

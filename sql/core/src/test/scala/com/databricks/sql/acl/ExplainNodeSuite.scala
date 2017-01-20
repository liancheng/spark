/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.execution.command.ExplainCommand

/**
 * Tests for adding/removing explain nodes.
 */
class ExplainNodeSuite extends PlanTest {
  test("add explain node") {
    val in = ExplainCommand(OneRowRelation)
    val out = ExplainCommand(ExplainNode(OneRowRelation))
    comparePlans(AddExplainNode(in), out)
    comparePlans(AddExplainNode(AddExplainNode(in)), out)
  }

  test("remove explain node") {
    val in = ExplainNode(OneRowRelation)
    val out = OneRowRelation
    comparePlans(CleanUpExplainNode(in), out)
  }
}

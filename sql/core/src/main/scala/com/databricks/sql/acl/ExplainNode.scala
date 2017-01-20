/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.ExplainCommand

/**
 * A header node for a to-be analyzed [[LogicalPlan]]. This allows us to apply less stringent
 * permission checks, to the [[LogicalPlan]].
 */
case class ExplainNode(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def treeString(verbose: Boolean): String = child.treeString(verbose)
}

/**
 * Rule that adds an ExplainNode as a top node of an [[ExplainCommand]]'s to be
 * explained [[LogicalPlan]].
 */
case object AddExplainNode extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case explain: ExplainCommand =>
      explain.logicalPlan match {
        case node: ExplainNode => explain
        case node => explain.copy(logicalPlan = ExplainNode(node))
      }
    case _ => plan
  }
}

case object CleanUpExplainNode extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case ExplainNode(child) => child
    case _ => plan
  }
}

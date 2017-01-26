/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._

/**
 * A trusted runnable command that we trust as soon its permission check out. In practice this
 * means that we do not check permissions for the command or for commands that the command issues
 * when the command is executed. An example of the latter is a CreateTableAsSelect command that
 * both issues a create and an insert command.
 */
case class TrustedRunnableCommand(cmd: RunnableCommand) extends RunnableCommand {
  assert(cmd != null)

  override def output: Seq[Attribute] = cmd.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    CheckPermissions.trusted(cmd.run(sparkSession))
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String): StringBuilder = {
    cmd.generateTreeString(depth, lastChildren, builder, verbose, prefix)
  }
}

/**
 * This rule wraps a RunnableCommand with a TrustedRunnableCommand before the command is executed.
 */
object MakeRunnableCommandTrusted extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case ExecutedCommandExec(_: TrustedRunnableCommand) => plan
    case ExecutedCommandExec(_: ExplainCommand) => plan
    case ExecutedCommandExec(_: ShowTablesCommand) => plan
    case ExecutedCommandExec(_: DescribeTableCommand) => plan
    case ExecutedCommandExec(cmd) => ExecutedCommandExec(TrustedRunnableCommand(cmd))
    case _ => plan
  }
}

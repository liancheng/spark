/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.execution.closure

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.expressions.closure.ClosureTranslation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Filter, LogicalPlan, MapElements, Project, SerializeFromObject, TypedFilter, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.closure.TranslateClosureOptimizerRule.Parent
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

/**
 * Rule to translates Dataset typed operation like map[T => U] or filter[T] to untyped operation.
 *
 * NOTE: This rule must be applied immediately after the analysis stage.
 *
 * For example, typed filter operation:
 * {{{
 *   spark.conf.set("spark.sql.translateClosure", "true")
 *   val ds = (0 to 10).toDS
 *   val query = ds.filter(_ > 5)
 * }}}
 *
 * is translated to untyped filter operation:
 * {{{
 *   scala> res1.explain()
 *   == Physical Plan ==
 *   *Filter (value#1 > 5)
 *   +- LocalTableScan [value#1]
 * }}}
 *
 * Typed map operation:
 * {{{
 *
 *   spark.conf.set("spark.sql.translateClosure", "true")
 *   val ds = spark.sparkContext.parallelize((0 to 10)).toDS
 *   val query = ds2.map(_ * 2)
 * }}}
 *
 * is translated to a Project operation:
 * {{{
 *   scala> query.explain()
 *   == Physical Plan ==
 *   *Project [(value#29 * 2) AS value#34]
 *   +- *SerializeFromObject [input[0, int, true] AS value#29]
 *      +- Scan ExternalRDDScan[obj#28]
 * }}}
 *
 */
case class TranslateClosureOptimizerRule(conf: SQLConf) extends Rule[LogicalPlan] {

  private def isConfigSet: Boolean = {
    // Enables the closure translation by default unless user have set
    // spark.conf.set("spark.sql.translateClosure", "false") explicitly
    conf.getConfString(TranslateClosureOptimizerRule.CONFIG_KEY, "false") == "true"
  }

  def apply(root: LogicalPlan): LogicalPlan = {
    val rule = (parent: LogicalPlan) => parent.mapChildren {
      // Translates the MapElements to Project.
      case serializer @ SerializeFromObject(_, map @ MapElements(func, argumentType, argumentSchema,
        _, DeserializeToObject(_, _, child)))
        if shouldTranslate(map, parent, child, argumentSchema) =>
        logTrace(s"Try to translate map function to expression in plan:\n ${map.treeString}")
        val resolvedExpressions = ClosureTranslation
          .translateMap(func, argumentType, argumentSchema).flatMap(resolve(_, child))
        // The Project should output attributes with same exprIds as SerializeFromObject.
        renameTo(resolvedExpressions, serializer.output) match {
          case Some(expressions) => Project(expressions, child)
          case None =>
            logInfo(
              s"""Failed to translate typed map operation to untyped operation.
                  |Current plan is: ${map.simpleString}.
                  |""".stripMargin)
            serializer
        }
      // Translates the TypedFilter to Filter.
      case filter @ TypedFilter(func, clazz, argumentSchema, _, child)
        if shouldTranslate(filter, parent, child, argumentSchema) =>
        logTrace(s"Try to translate filter function to expression in plan:\n ${filter.treeString}")
        val filterExpression = ClosureTranslation.translateFilter(func, clazz, argumentSchema)
        val untypedFilter = filterExpression match {
          case Some(expression) => resolve(expression, child).map(Filter(_, child))
          case None =>
            logInfo(
              s"""Failed to translate typed filter operation to untyped operation.
                  |Current plan is: ${filter.simpleString}.
                  |""".stripMargin)
            None
        }
        untypedFilter.getOrElse(filter)
      case other => other
    }

    // Parent(root).children(0) == root. Here we use a fake Parent node so that we can transform
    // root node to a new root node by applying mapChildren recursively.
    Parent(root).transformDown(PartialFunction(rule)).transformUp(PartialFunction(rule))
      .children(0)
  }

  // Renames expression using existing name and ExprId of newNames
  private def renameTo(exprs: Seq[Expression], newNames: Seq[Attribute]): Option[Seq[Alias]] = {
    if (exprs.length != newNames.length) {
      None
    } else {
      Some(exprs.zip(newNames).map { kv =>
        val (expression, newName) = kv
        Alias(expression, newName.name)(exprId = newName.exprId)
      })
    }
  }

  // Resolves all UnresolvedAttribute
  private def resolve(expr: Expression, child: LogicalPlan): Option[Expression] = {
    val resolved = expr.transformUp {
      case u @ UnresolvedAttribute(nameParts) =>
        child.resolve(nameParts, conf.resolver).getOrElse(u)
    }
    if (resolved.resolved) {
      Some(resolved)
    } else {
      logInfo(s"Failed to resolve all UnresolvedAttribute in expression ${expr}")
      None
    }
  }

  /**
   * Checks feature flag and does schema verification to make sure this conversion is supported. It
   * also test whether current plan is located at the boundary of typed/untyped operation. Each
   * time we will try to translate one typed operator at the boundary to untyped operator,
   * and repeat this process until the plan is stable.
   *
   * For example, for query:
   * {{{
   *   case class A(a: Int)
   *   val ds = Seq(1,2,3)
   *     .toDF
   *     .select($"value" * 1 as "a")    // <-- untyped operation
   *     .filter($"a" > 2)               // <-- untyped operation
   *     .as[A]
   *     .filter(_.a > 3)               // <-- typed operation at the typed/untyped boundary
   *     .filter(_.a > 4)           // <-- typed operation
   *     .map(_.a * 5)                   // <-- typed operation at the typed/untyped boundary
   *     .filter($"value" > 6)           // <-- untyped operation
   * }}}
   *
   * The logical plan is:
   * {{{
   *   Filter (value#197 > 6)                                    <-- untyped operator
   *     +- SerializeFromObject
   *        +- MapElements (_.a * 5)                             <-- boundary typed operator
   *           +- DeserializeToObject
   *              +- TypedFilter (_.a > 4)                       <-- typed operator
   *                  +- TypedFilter (_.a > 3),                  <-- boundary typed operator
   *                     +- Filter (a#187 > 2)                   <-- untyped operator
   *                        +- Project [(value#183 * 1) AS a#187]<-- untyped operator
   *                           +- LocalRelation [value#183]
   * }}}
   *
   * After translating typed operators at the boundary, we get:
   * {{{
   *   Filter (value#257 > 6)                             <-- untyped operator
   *     +- Project [(a#246 * 5) AS value#257]            <-- untyped, translated from MapElements
   *        +- TypedFilter (_.a > 4)                      <-- typed, new boundary
   *           +- Filter (a#246 > 3)                      <-- untyped, translated from TypedFilter
   *              +- Filter (a#246 > 2)                   <-- untyped operator
   *                 +- Project [(value#242 * 1) AS a#246]<-- untyped operator
   *                     +- LocalRelation [value#242]
   * }}}
   *
   * We repeats this process to translate boundary typed operator to untyped operator until the
   * whole plan is stable. For more info, please see the design doc.
   */
  private def shouldTranslate(
      current: LogicalPlan,
      parent: LogicalPlan,
      child: LogicalPlan,
      argumentSchema: StructType): Boolean = {

    if (isConfigSet) {
      val isSchemaMatch = schemaMatch(child, argumentSchema)

      // If parent is not typed, or child is not typed, it means current operator lies at the
      // boundary of typed and untyped operators. Then this typed operator can be safely translated
      // to untyped operator, without requiring new serialization/de-serialization operators.
      val isBoundary = !isTyped(parent) || !isTyped(child)
      if (!isBoundary) {
        logInfo(
          s"""Closure translation is disabled because current typed operator is not located at the
              |boundary of typed/untyped operators.
              |Current plan is: ${current.simpleString}
              |""".stripMargin)
      }

      isSchemaMatch && isBoundary
    } else {
      false
    }
  }

  // Tests if all fields of argumentSchema can be found in logicalPlan's schema
  private def schemaMatch(child: LogicalPlan, argumentSchema: StructType): Boolean = {

    // Returns true if containee is a sub-tree of container
    def contains(container: StructField, containee: StructField): Boolean = {
      (container.dataType, containee.dataType) match {
        case (l: AtomicType, r: AtomicType) =>
          l == r && container.name == containee.name
        case (l: StructType, r: StructType) =>
          r.fieldNames.forall { fieldName =>
            if (l.fieldNames.count(_ == fieldName) != 1) {
              // Ambiguous field name
              false
            } else {
              val leftField = l.getFieldIndex(fieldName).map(l.fields(_))
              val rightField = r.getFieldIndex(fieldName).map(r.fields(_))
              leftField.isDefined &&
                rightField.isDefined &&
                contains(leftField.get, rightField.get)
            }
          }
        case _ => false
      }
    }
    val logicalPlanSchema = child.schema
    val isSchemaMatch = contains(
      // Wrap the top level struct type in a StructField
      container = StructField("toplevel", logicalPlanSchema),
      containee = StructField("toplevel", argumentSchema))

    if (!isSchemaMatch) {
      logInfo(
        s"""Closure translation is disabled because schema mismatch when translating closure.
           |Child plan's schema is: ${child.schema}
           |argument type T's schema is: ${argumentSchema}
           |""".stripMargin)
    }
    isSchemaMatch
  }

  // Checks whether this plan is a typed operator like TypedFilter. Other typed operation is wrapped
  // inside SerializeFromObject/DeserializeToObject pair.
  private def isTyped(plan: LogicalPlan): Boolean = {
    plan match {
      case _: SerializeFromObject => true
      case _: DeserializeToObject => true
      case _: TypedFilter => true
      case _ => false
    }
  }
}

object TranslateClosureOptimizerRule {

  // A helper class for traversing the logical plan tree.
  case class Parent(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }

  val CONFIG_KEY = "spark.sql.translateClosure"
}

/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import org.slf4j.LoggerFactory

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.MetadataBuilder


/** Package-level static methods and variable constants. These includes helper functions for
 * adding and converting expressions, formatting blocks and identifiers, logging, and
 * formatting SQL.
 */
package object pushdown {
  /** This wraps all identifiers with the following symbol. */
  private final val QUOTE_CHAR = "\""

  private[pushdown] final val log = LoggerFactory.getLogger(getClass)

  /** Query blocks. */
  private[pushdown] final def block(text: String): String = {
    "(" + text + ")"
  }

  /** Same as block() but with an alias. */
  private[pushdown] final def block(text: String, alias: String): String = {
    block(text) + " AS " + wrap(alias)
  }

  /** This adds an attribute as part of a SQL expression, searching in the provided
   * fields for a match, so the subquery qualifiers are correct.
   *
   * @param attr The Spark Attribute object to be added.
   * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
   *               usually derived from the output of a subquery.
   * @return A string representing the attribute expression.
   */
  private[pushdown] final def addAttribute(
      attr: Attribute,
      fields: Seq[Attribute]): String = {
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttribute(resolved.qualifier, resolved.name)
      case None => qualifiedAttribute(attr.qualifier, attr.name)
    }
  }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[pushdown] final def qualifiedAttribute(
      alias: Option[String],
      name: String) = {
    val str = alias match {
      case Some(qualifier) => wrap(qualifier) + "."
      case None => ""
    }

    if (name.startsWith(QUOTE_CHAR) && name.endsWith(QUOTE_CHAR)) str + name
    else str + wrap(name)
  }

  private[pushdown] final def wrap(name: String): String = {
    QUOTE_CHAR + name + QUOTE_CHAR
  }

  private[pushdown] def renameColumns(
      origOutput: Seq[NamedExpression],
      alias: String): Seq[NamedExpression] = {

    val col_names = Iterator.from(0).map(n => s"col_$n")

    origOutput.map { expr =>
      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a @ Alias(child: Expression, name: String) =>
          Alias(child, altName)(a.exprId, None, Some(expr.metadata))
        case _ =>
          Alias(expr, altName)(expr.exprId, None, Some(expr.metadata))
      }
    }
  }
}

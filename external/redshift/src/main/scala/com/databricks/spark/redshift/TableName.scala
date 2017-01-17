/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.spark.redshift

import scala.collection.mutable.ArrayBuffer

/**
 * Wrapper class for representing the name of a Redshift table.
 */
private[redshift] case class TableName(unescapedSchemaName: String, unescapedTableName: String) {
  private def quote(str: String) = '"' + str.replace("\"", "\"\"") + '"'
  def escapedSchemaName: String = quote(unescapedSchemaName)
  def escapedTableName: String = quote(unescapedTableName)
  override def toString: String = s"$escapedSchemaName.$escapedTableName"
}

private[redshift] object TableName {
  /**
   * Parses a table name which is assumed to have been escaped according to Redshift's rules for
   * delimited identifiers.
   */
  def parseFromEscaped(str: String): TableName = {
    def dropOuterQuotes(s: String) =
      if (s.startsWith("\"") && s.endsWith("\"")) s.drop(1).dropRight(1) else s
    def unescapeQuotes(s: String) = s.replace("\"\"", "\"")
    def unescape(s: String) = unescapeQuotes(dropOuterQuotes(s))
    splitByDots(str) match {
      case Seq(tableName) => TableName("PUBLIC", unescape(tableName))
      case Seq(schemaName, tableName) => TableName(unescape(schemaName), unescape(tableName))
      case other => throw new IllegalArgumentException(s"Could not parse table name from '$str'")
    }
  }

  /**
   * Split by dots (.) while obeying our identifier quoting rules in order to allow dots to appear
   * inside of quoted identifiers.
   */
  private def splitByDots(str: String): Seq[String] = {
    val parts: ArrayBuffer[String] = ArrayBuffer.empty
    val sb = new StringBuilder
    var inQuotes: Boolean = false
    for (c <- str) c match {
      case '"' =>
        // Note that double quotes are escaped by pairs of double quotes (""), so we don't need
        // any extra code to handle them; we'll be back in inQuotes=true after seeing the pair.
        sb.append('"')
        inQuotes = !inQuotes
      case '.' =>
        if (!inQuotes) {
          parts.append(sb.toString())
          sb.clear()
        } else {
          sb.append('.')
        }
      case other =>
        sb.append(other)
    }
    if (sb.nonEmpty) {
      parts.append(sb.toString())
    }
    parts
  }
}

/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.catalyst.expressions.closure

class ClosureTranslationException(
    message: String,
    ex: Throwable = null)
  extends Exception(message, ex)

class ByteCodeParserException(message: String) extends ClosureTranslationException(message, null)

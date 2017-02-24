/*
 * Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql

import com.databricks.sql.parser.DatabricksSqlParser

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Object holding together Databricks-specific extensions that will be enabled by default.
 */
object DatabricksExtensions extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (_, delegate) =>
      new DatabricksSqlParser(None, delegate)
    }
  }
}

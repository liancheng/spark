/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks

/**
 * This lets users refer to the class via com.databricks.DatabricksAtomicCommitProtocol, while we
 * keep the implementation under org.apache.spark to access Spark internal classes.
 */
class DatabricksAtomicCommitProtocol(jobId: String, path: String)
  extends org.apache.spark.sql.transaction.DatabricksAtomicCommitProtocol(jobId, path)

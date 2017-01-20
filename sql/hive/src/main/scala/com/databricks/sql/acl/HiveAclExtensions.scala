/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Databricks Spark ACL configuration object for Hive. In order to use this, you will need to pass
 * the following configurations when constructing the SparkSession:
 * 1. spark.sql.extensions = "com.databricks.sql.acl.HiveAclExtensions"
 * 2. spark.databricks.acl.provider = "[Name of an AclProvider]"
 * 3. spark.databricks.acl.client = "[Name of the DB side ACL client]". This is only needed when
 *    we use the [[ReflectionBackedAclProvider]] as the provider.
 */
class HiveAclExtensions extends AclExtensions {
  /**
   * Create a hive enabled permission checker.
   */
  override protected def createPermissionsCheck(session: SparkSession): (LogicalPlan) => Unit = {
    new HiveCheckPermissions(session.catalog, client(session))
  }
}

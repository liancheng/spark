/*
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.sql.acl

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Databricks Spark ACL configuration object. In order to use this, you will need to pass the
 * following configurations when constructing the SparkSession:
 * 1. spark.sql.extensions = "com.databricks.sql.acl.AclExtensions"
 * 2. spark.databricks.acl.provider = "[Name of an AclProvider]"
 * 3. spark.databricks.acl.client = "[Name of the DB side ACL client]". This is only needed when
 *    we use the [[ReflectionBackedAclProvider]] as the provider.
 */
class AclExtensions extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectAnalyzerRule { _ =>
      AddExplainNode
    }
    extensions.injectAnalyzerRule { session =>
      new ResolveDropCommand(session)
    }
    extensions.injectCheckAnalysisRule(createPermissionsCheck)
    extensions.injectOptimizerRule { _ =>
      CleanUpExplainNode
    }
    extensions.injectParser { (session, delegate) =>
      new AclCommandParser(client(session), delegate)
    }
    extensions.injectCatalogHooks { session =>
      new AclIntegratedCatalogHooks(client(session))
    }
  }

  /**
   * Create an [[AclClient]]. This is only visible for testing.
   */
  protected def client(session: SparkSession): AclClient = AclProvider.client(session)

  /**
   * Create a permission checker. This rule is dependent on the use of sql-core or hive, and needs
   * to be modifiable.
   */
  protected def createPermissionsCheck(session: SparkSession): LogicalPlan => Unit = {
    new CheckPermissions(session.catalog, client(session))
  }
}

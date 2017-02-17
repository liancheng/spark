/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.databricks.sql

import org.apache.spark.sql.internal.SQLConf.buildConf
import org.apache.spark.sql.internal.SQLConf.buildStaticConf


/**
 * Configurations for Databricks edge features.
 */
object DatabricksSQLConf {

  val FILES_ASYNC_IO = buildConf("spark.sql.files.asyncIO")
    .internal()
    .doc("If true, attempts to asynchronously do IO when reading data.")
    .booleanConf
    .createWithDefault(true)

  val DYNAMIC_PARTITION_PRUNING = buildConf("spark.sql.dynamicPartitionPruning")
    .internal()
    .doc("When true, we will generate predicate for partition column when it's used as join key")
    .booleanConf
    .createWithDefault(true)

  /**
   * Use an optimizer rule doing advanced query pushdown into Redshift.
   *
   * The rule is injected into extraOptimizations of the [[org.apache.spark.sql.SparkSession]]
   * the first time a RedshiftRelation is constructed.
   */
  val REDSHIFT_ADVANCED_PUSHDOWN = buildConf("spark.databricks.redshift.pushdown")
    .internal()
    .doc("When true, advanced query pushdown into Redshift is used.")
    .booleanConf
    .createWithDefault(true)

}


/**
 * List of static (immutable during runtime) configs for edge features.
 */
object DatabricksStaticSQLConf {

  val ACL_PROVIDER = buildStaticConf("spark.databricks.acl.provider")
    .internal()
    .doc("Name of the AclProvider. This class is responsible for creating an AclClient. This " +
      "class should implement the com.databricks.sql.acl.AclProvider trait and provide a " +
      "no-args constructor.")
    .stringConf
    .createOptional

  val ACL_CLIENT_BACKEND = buildStaticConf("spark.databricks.acl.client")
    .internal()
    .doc("Name of the ACL client backend used by the ReflectionBackedAclClient.")
    .stringConf
    .createOptional

  val ACL_ENABLED = buildStaticConf("spark.databricks.acl.enabled")
    .internal()
    .doc("Whether the SQL-based Access Control is enabled.")
    .booleanConf
    .createWithDefault(false)

}

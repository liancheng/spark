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

  val FILES_ASYNC_IO =
    buildConf("spark.sql.files.asyncIO")
      .internal()
      .doc("If true, attempts to asynchronously do IO when reading data.")
      .booleanConf
      .createWithDefault(true)

  val DYNAMIC_PARTITION_PRUNING =
    buildConf("spark.sql.dynamicPartitionPruning")
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
  val REDSHIFT_ADVANCED_PUSHDOWN =
    buildConf("spark.databricks.redshift.pushdown")
      .internal()
      .doc("When true, advanced query pushdown into Redshift is used.")
      .booleanConf
      .createWithDefault(true)

  val DIRECTORY_COMMIT_FILTER_UNCOMMITTED =
    buildConf("spark.databricks.directoryCommit.enableFilterUncommitted")
      .internal()
      .doc("If true, enable the read protocol, ensuring that files pertaining to uncommitted " +
        "transactions are filtered out.")
      .booleanConf
      .createWithDefault(true)

  val DIRECTORY_COMMIT_IGNORE_CORRUPT_MARKERS =
    buildConf("spark.databricks.directoryCommit.ignoreCorruptCommitMarkers")
      .internal()
      .doc("If true, unreadable commit markers will be ignored rather than raising an error.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_COMMIT_ENABLE_LOGICAL_DELETE =
    buildConf("spark.databricks.directoryCommit.enableLogicalDelete")
      .internal()
      .doc("Flag specifying whether or not atomic overwrites should be enabled.")
      .booleanConf
      .createWithDefault(true)

  val DIRECTORY_COMMIT_AUTO_VACUUM_ON_COMMIT =
    buildConf("spark.databricks.directoryCommit.autoVacuumOnCommit")
      .internal()
      .doc("If true, every Commit will trigger a Vacuum operation on all the affected paths.")
      .booleanConf
      .createWithDefault(true)

  val DIRECTORY_COMMIT_VACUUM_DATA_HORIZON_HRS =
    buildConf("spark.databricks.directoryCommit.vacuum.dataHorizonHours")
      .internal()
      .doc("Pending jobs which are older than the specified number of hours will be considered " +
        "failed and any files written by them will be vacuumed as well.")
      .doubleConf
      .createWithDefault(48.0) // 2 days

  val DIRECTORY_COMMIT_VACUUM_METADATA_HORIZON_HRS =
    buildConf("spark.databricks.directoryCommit.vacuum.metadataHorizonHours")
      .internal()
      .doc("Vacuum will remove commit markers that are older than this number of hours. " +
        "This should be greater than the max amount of time we think a zombie executor can " +
        "hang around and write output after the job has finished.")
      .doubleConf
      .createWithDefault(0.5) // 30 minutes

  val DIRECTORY_COMMIT_WRITE_REORDERING_HORIZON_MS =
    buildConf("spark.databricks.directoryCommit.writeReorderingHorizon")
      .internal()
      .doc("A List operation is considered unaffected by write reordering issues if all files " +
        "are older than the specified number of milliseconds. Otherwise an extra List is issued.")
      .longConf
      .createWithDefault(5 * 60 * 1000) // 5 minutes
}


/**
 * List of static (immutable during runtime) configs for edge features.
 */
object DatabricksStaticSQLConf {

  val ACL_PROVIDER =
    buildStaticConf("spark.databricks.acl.provider")
      .internal()
      .doc("Name of the AclProvider. This class is responsible for creating an AclClient. This " +
        "class should implement the com.databricks.sql.acl.AclProvider trait and provide a " +
        "no-args constructor.")
      .stringConf
      .createOptional

  val ACL_CLIENT_BACKEND =
    buildStaticConf("spark.databricks.acl.client")
      .internal()
      .doc("Name of the ACL client backend used by the ReflectionBackedAclClient.")
      .stringConf
      .createOptional

  val ACL_ENABLED =
    buildStaticConf("spark.databricks.acl.enabled")
      .internal()
      .doc("Whether the SQL-based Access Control is enabled.")
      .booleanConf
      .createWithDefault(false)

}

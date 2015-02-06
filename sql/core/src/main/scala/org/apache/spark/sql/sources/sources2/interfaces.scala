package org.apache.spark.sql.sources.sources2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

trait BaseRelation {
  private[sources2] type RelationDecorator = PartialFunction[BaseRelation, BaseRelation]
}

trait ScanSupport extends BaseRelation {
  def scan(): RDD[Row]
}

trait ColumnPruningSupport extends ScanSupport {
  def setRequiredColumns(columns: Array[String]): Unit
}

trait FilterPushDownSupport extends ScanSupport {
  def setFilters(filters: Array[Filter]): Unit
}

case class Partition(values: Row, path: String)
case class PartitionSpec(partitionColumns: StructType, partitions: Array[Partition])

trait PartitionDiscoverySupport extends ScanSupport {
  def parsePartitions(path: String): PartitionSpec
}

trait InsertionSupport extends BaseRelation {
  def setOverwrite(overwrite: Boolean): Unit
  def setDestination(path: String): Unit
  def insert(data: DataFrame): Unit
}

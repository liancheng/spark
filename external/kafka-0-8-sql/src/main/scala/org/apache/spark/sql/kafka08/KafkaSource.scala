/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.kafka08

import java.{util => ju}
import java.io._
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import kafka.common.TopicAndPartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka08.KafkaSource._
import org.apache.spark.sql.types._

/**
 * A [[Source]] that uses Kafka 0.8 APIs to reads data from Kafka. The design for this source
 * follows `org.apache.spark.sql.kafka010.KafkaSource` except:
 *
 * - Don't support `subscribePattern`. Because without the 0.10 Kafka APIs, we need to ask all
 *   topics from Zookeeper and filter topics by ourselves.
 * - Don't support `failOnDataLoss` option. It means that the user cannot delete topics, otherwise
 *   the query will fail.
 */
private[kafka08] class KafkaSource(
    sqlContext: SQLContext,
    consumerStrategy: ConsumerStrategy,
    kafkaParams: ju.Map[String, String],
    sourceOptions: Map[String, String],
    metadataPath: String,
    startingOffsets: StartingOffsets)
  extends Source with Logging {

  private val sc = sqlContext.sparkContext

  @transient private val kc = new KafkaCluster(kafkaParams)

  private val maxOffsetFetchAttempts =
    sourceOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private val offsetFetchAttemptIntervalMs =
    sourceOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  private val maxOffsetsPerTrigger =
    sourceOptions.get("maxOffsetsPerTrigger").map(_.toLong)

  private lazy val initialPartitionOffsets = {
    val metadataLog =
      new HDFSMetadataLog[KafkaSourceOffset](sqlContext.sparkSession, metadataPath) {
        override def serialize(metadata: KafkaSourceOffset, out: OutputStream): Unit = {
          val bytes = metadata.json.getBytes(StandardCharsets.UTF_8)
          out.write(bytes.length)
          out.write(bytes)
        }

        override def deserialize(in: InputStream): KafkaSourceOffset = {
          val length = in.read()
          val bytes = new Array[Byte](length)
          in.read(bytes)
          KafkaSourceOffset(SerializedOffset(new String(bytes, StandardCharsets.UTF_8)))
        }
      }

    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsets => KafkaSourceOffset(fetchEarliestOffsets())
        case LatestOffsets => KafkaSourceOffset(fetchLatestOffsets())
        case SpecificOffsets(p) => KafkaSourceOffset(fetchSpecificStartingOffsets(p))
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  private var currentPartitionOffsets: Option[Map[TopicAndPartition, Long]] = None

  override def schema: StructType = KafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val latest = fetchLatestOffsets()
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest
      case Some(limit) if currentPartitionOffsets.isEmpty =>
        rateLimit(limit, initialPartitionOffsets, latest)
      case Some(limit) =>
        rateLimit(limit, currentPartitionOffsets.get, latest)
    }

    currentPartitionOffsets = Some(offsets)
    logDebug(s"GetOffset: ${offsets.toSeq.map(_.toString).sorted}")
    Some(KafkaSourceOffset(offsets))
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: Map[TopicAndPartition, Long],
      until: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    val fromNew = fetchNewPartitionEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.get(tp).getOrElse(fromNew(tp))
            val prorate = limit * (size / total)
            logDebug(s"rateLimit $tp prorated amount is $prorate")
            // Don't completely starve small topicpartitions
            val off = begin + (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            logDebug(s"rateLimit $tp new offset is $off")
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    logInfo(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionOffsets = fetchNewPartitionEarliestOffsets(newPartitions.toSeq)
    if (newPartitionOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionOffsets.keySet)
      throw new IllegalStateException(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionOffsets")
    newPartitionOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      throw new IllegalStateException(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      throw new IllegalStateException(
        s"$deletedPartitions are gone. Some data may have been missed")
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = topicPartitions.map { tp =>
      val fromOffset = fromPartitionOffsets.get(tp).getOrElse {
        newPartitionOffsets.getOrElse(tp, {
          // This should not happen since newPartitionOffsets contains all partitions not in
          // fromPartitionOffsets
          throw new IllegalStateException(s"$tp doesn't have a from offset")
        })
      }
      val untilOffset = untilPartitionOffsets(tp)
      KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        throw new IllegalStateException(
          s"Partition ${range.topicPartition}'s offset was changed from " +
          s"${range.fromOffset} to ${range.untilOffset}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = new KafkaSourceRDD(
      sc,
      kafkaParams,
      offsetRanges,
      maxOffsetFetchAttempts,
      offsetFetchAttemptIntervalMs)

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = Some(untilPartitionOffsets)
    }

    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {}

  override def toString(): String = s"KafkaSource[$consumerStrategy]"

  /**
   * Set consumer position to specified offsets, making sure all assignments are set.
   */
  private def fetchSpecificStartingOffsets(
      partitionOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = withRetries {
    partitionOffsets.toSeq.groupBy {
      case (_, off) if off != -1 && off != -2 => 0
      case (_, off) => off
    }.flatMap {
      case (key, offsets) if key == -1 => kc.getLatestOffsets(offsets.toMap.keySet)
      case (key, offsets) if key == -2 => kc.getEarliestOffsets(offsets.toMap.keySet)
      case (_, offsets) => offsets
    }
  }

  /**
   * Fetch the earliest offsets of partitions.
   */
  private def fetchEarliestOffsets(): Map[TopicAndPartition, Long] = withRetries {
    kc.getEarliestOffsets(consumerStrategy.topicPartitions(kc))
  }

  /**
   * Fetch the latest offset of partitions.
   */
  private def fetchLatestOffsets(): Map[TopicAndPartition, Long] = withRetries {
    kc.getLatestOffsets(consumerStrategy.topicPartitions(kc))
  }

  /**
   * Fetch the earliest offsets for newly discovered partitions. The return result may not contain
   * some partitions if they are deleted.
   */
  private def fetchNewPartitionEarliestOffsets(
      newPartitions: Seq[TopicAndPartition]): Map[TopicAndPartition, Long] = withRetries {
    kc.getEarliestOffsets(newPartitions.toSet)
  }

  /**
   * Helper function that does multiple retries on the a body of code that returns offsets.
   * Retries are needed to handle transient failures, e.g., LeaderNotAvailableException.
   */
  private def withRetries(body: => Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    synchronized {
      var result: Option[Map[TopicAndPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts) {
        try {
          result = Some(body)
        } catch {
          case NonFatal(e) =>
            lastException = e
            logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
            attempt += 1
            Thread.sleep(offsetFetchAttemptIntervalMs)
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException()
      }
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }
}


/** Companion object for the [[KafkaSource]]. */
private[kafka08] object KafkaSource {

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))

  sealed trait ConsumerStrategy {
    def topicPartitions(kc: KafkaCluster): Set[TopicAndPartition]
  }

  case class AssignStrategy(partitions: Set[TopicAndPartition]) extends ConsumerStrategy {
    override def topicPartitions(kc: KafkaCluster): Set[TopicAndPartition] = partitions

    override def toString: String = s"Assign[${partitions.mkString(", ")}]"
  }

  case class SubscribeStrategy(topics: Set[String]) extends ConsumerStrategy {
    override def topicPartitions(kc: KafkaCluster): Set[TopicAndPartition] = {
      kc.getPartitions(topics)
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }
}

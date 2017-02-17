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

import scala.collection.mutable.ArrayBuffer

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common._
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

private[kafka08] case class KafkaSourceRDDOffsetRange(
    topicPartition: TopicAndPartition,
    fromOffset: Long,
    untilOffset: Long) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  def size: Long = untilOffset - fromOffset
}

private[kafka08] case class KafkaSourceRDDPartition(
  index: Int, offsetRange: KafkaSourceRDDOffsetRange) extends Partition

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 *
 * The original file is `org.apache.spark.sql.kafka010.KafkaSourceRDD`.
 *
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka08] class KafkaSourceRDD(
    sc: SparkContext,
    kafkaParams: ju.Map[String, String],
    offsetRanges: Seq[KafkaSourceRDDOffsetRange],
    maxOffsetFetchAttempts: Int,
    offsetFetchAttemptIntervalMs: Long) extends RDD[InternalRow](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaSourceRDDPartition(i, o) }.toArray
  }

  override def count: Long = offsetRanges.map(_.size).sum

  override def countApprox(
    timeout: Long,
    confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }

  override def isEmpty: Boolean = count == 0

  override def take(num: Int): Array[InternalRow] = {
    val nonEmptyPartitions = this.partitions
      .map(_.asInstanceOf[KafkaSourceRDDPartition])
      .filter(_.offsetRange.size > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return Array.empty
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.offsetRange.size)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[InternalRow]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[InternalRow]) => it.take(parts(tc.partitionId)).toArray,
      parts.keys.toArray)
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    val range = thePart.asInstanceOf[KafkaSourceRDDPartition].offsetRange
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      new KafkaSourceRDDIterator(range, context)
    }
  }

  /**
   * An iterator that fetches messages directly from Kafka for the offsets in partition.
   */
  private class KafkaSourceRDDIterator(
      range: KafkaSourceRDDOffsetRange,
      context: TaskContext) extends NextIterator[InternalRow] {

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    logInfo(s"Computing topic ${range.topic}, partition ${range.partition} " +
      s"offsets ${range.fromOffset} -> ${range.untilOffset}")

    private val topicUTF8 = UTF8String.fromString(range.topic)
    private val kc = new KafkaCluster(kafkaParams)
    private val consumer = withRetries {
      kc.connectLeader(range.topic, range.partition)
    }

    private var requestOffset = range.fromOffset
    private var iter: Iterator[MessageAndOffset] = null

    /**
     * Run `body` and retry upon `LeaderNotAvailableException`, `NotLeaderForPartitionException` or
     * `ReplicaNotAvailableException`.
     */
    private def withRetries[T](body: => T): T = {
      var result: Option[T] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts) {
        try {
          result = Some(body)
        } catch {
          case e@(_: LeaderNotAvailableException |
                  _: NotLeaderForPartitionException |
                  _: ReplicaNotAvailableException) =>
            lastException = e
            logWarning(s"Error in attempt $attempt connecting to Kafka: ", e)
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

    private def fetchBatch: Iterator[MessageAndOffset] = {
      val req = new FetchRequestBuilder()
        .addFetch(range.topic, range.partition, requestOffset, kc.config.fetchMessageMaxBytes)
        .build()
      val resp = withRetries {
        val _resp = consumer.fetch(req)
        assert(_resp.data.size == 1)
        if (_resp.hasError) {
          val err = _resp.errorCode(range.topic, range.partition)
          throw ErrorMapping.exceptionFor(err)
        }
        _resp
      }

      // kafka may return a batch that starts before the requested offset
      resp.messageSet(range.topic, range.partition)
        .iterator
        .dropWhile(_.offset < requestOffset)
    }

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    override def getNext(): InternalRow = {
      if (iter == null || !iter.hasNext) {
        iter = fetchBatch
      }
      if (!iter.hasNext) {
        if (requestOffset != range.untilOffset) {
          throw new IllegalStateException(
            s"Cannot fetch records in [$requestOffset, ${range.untilOffset}) for "
              + s"${range.topicPartition}")
        }
        finished = true
        null
      } else {
        val item = iter.next()
        if (item.offset >= range.untilOffset) {
          if (item.offset != range.untilOffset) {
            throw new IllegalStateException(
              s"Cannot fetch records in [${range.untilOffset}, ${item.offset}) for "
                + s"${range.topicPartition}")
          }
          finished = true
          null
        } else {
          if (requestOffset != item.offset) {
            assert(requestOffset < item.offset) // guaranteed by "fetchBatch"
            throw new IllegalStateException(
              s"Cannot fetch records in [$requestOffset, ${item.offset}) for "
                + s"${range.topicPartition}")
          }
          requestOffset = item.nextOffset
          createInternalRow(item)
        }
      }
    }

    private def createInternalRow(item: MessageAndOffset): InternalRow = {
      val key = item.message.key
      val value = item.message.payload
      InternalRow(
        if (key == null) null else JavaUtils.bufferToArray(key),
        if (value == null) null else JavaUtils.bufferToArray(value),
        topicUTF8,
        range.partition,
        item.offset,
        KafkaSourceRDD.NO_TIMESTAMP,
        KafkaSourceRDD.NO_TIMESTAMP_TYPE)
    }
  }
}

private[kafka08] object KafkaSourceRDD {
  // Kafka 0.8 doesn't provide timestamp information. We use the same values as
  // `org.apache.kafka.common.record.Record.NO_TIMESTAMP` and
  // `org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE` for consistency.
  val NO_TIMESTAMP = DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(-1L))
  val NO_TIMESTAMP_TYPE = -1
}

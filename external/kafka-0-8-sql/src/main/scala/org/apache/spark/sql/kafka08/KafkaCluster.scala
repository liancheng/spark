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
 * Copyright © 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.kafka08

import java.{util => ju}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import kafka.api._
import kafka.common.{ErrorMapping, TopicAndPartition, UnknownTopicOrPartitionException}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

/**
 * Convenience methods for interacting with a Kafka cluster.
 * See <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol">
 * A Guide To The Kafka Protocol</a> for more details on individual api calls.
 *
 * This class follows `org.apache.spark.streaming.kafka.KafkaCluster` except:
 *
 * - The APIs will ignore unknown topics and partitions. The caller should handle it.
 * - It throws Exception when verifying Kafka's repsonse immediately.
 * - Offset APIs return offsets directly instead of `LeaderOffset`s to use the same offset
 * format as Kafka 0.10 source.
 *
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 *                    configuration parameters</a>. Requires "metadata.broker.list" or
 *                    "bootstrap.servers" to be set with Kafka broker(s), NOT zookeeper servers,
 *                    specified in host1:port1,host2:port2 form
 */
class KafkaCluster(val kafkaParams: ju.Map[String, String]) extends Logging {
  import KafkaCluster.SimpleConsumerConfig

  val config: SimpleConsumerConfig = SimpleConsumerConfig(kafkaParams.asScala.toMap)

  private def connect(host: String, port: Int): SimpleConsumer =
    new SimpleConsumer(host, port, config.socketTimeoutMs,
      config.socketReceiveBufferBytes, config.clientId)

  def connectLeader(topic: String, partition: Int): SimpleConsumer = {
    val leader = findLeader(topic, partition)
    connect(leader._1, leader._2)
  }

  private def findLeader(topic: String, partition: Int): (String, Int) = {
    findLeaders(Set(TopicAndPartition(topic, partition))).values.headOption.getOrElse {
      throw new UnknownTopicOrPartitionException(s"topic: $topic partition: $partition")
    }
  }

  /**
   * Find the leaders of `TopicAndPartition`s. The unknown topics or partitions will be ignored.
   */
  private def findLeaders(
      topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, (String, Int)] = {
    if (topicAndPartitions.isEmpty) {
      return Map.empty
    }
    val tms = getTopicMetadata(topicAndPartitions.map(_.topic))
    val leaderMap = tms.flatMap(tm => tm.errorCode match {
      case ErrorMapping.NoError =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = TopicAndPartition(tm.topic, pm.partitionId)
          if (topicAndPartitions(tp)) {
            pm.errorCode match {
              case ErrorMapping.NoError =>
                // scalastyle:off
                // As per https://github.com/apache/kafka/blob/0.8.2.1/core/src/main/scala/kafka/server/MetadataCache.scala#L58
                // `pm.leader` should always be defined.
                // scalastyle:on
                assert(
                  pm.leader.isDefined,
                  s"Cannot find leader for topic ${tm.topic} partition ${pm.partitionId}")
                val leader = pm.leader.get
                Some(tp -> (leader.host, leader.port))
              case ErrorMapping.UnknownTopicOrPartitionCode =>
                None
              case errorCode =>
                throw ErrorMapping.exceptionFor(errorCode)
            }
          } else {
            // Ignore errors for non-requested partitions
            None
          }
        }
      case ErrorMapping.UnknownTopicOrPartitionCode =>
        None
      case errorCode =>
        throw ErrorMapping.exceptionFor(errorCode)
    }).toMap
    val missing = topicAndPartitions.diff(leaderMap.keySet)
    if (missing.nonEmpty) {
      logWarning(s"Unknown topic partitions: $missing")
    }
    leaderMap
  }

  /**
   * Return partitions for the specified topic set. Unknown topics or partitions will be ignored.
   */
  def getPartitions(topics: Set[String]): Set[TopicAndPartition] = {
    getTopicMetadata(topics).flatMap(tm => tm.errorCode match {
      case ErrorMapping.NoError =>
        tm.partitionsMetadata.filter(pm => pm.errorCode match {
          case ErrorMapping.NoError => true
          case ErrorMapping.UnknownTopicOrPartitionCode =>
            logDebug(s"Unknown topic: ${tm.topic} partition: ${pm.partitionId}")
            false
          case errorCode =>
            throw ErrorMapping.exceptionFor(errorCode)
        }).map(pm => TopicAndPartition(tm.topic, pm.partitionId))
      case ErrorMapping.UnknownTopicOrPartitionCode =>
        logDebug(s"Unknown topic: ${tm.topic}")
        Seq.empty
      case errorCode =>
        throw ErrorMapping.exceptionFor(errorCode)
    })
  }

  // Metadata api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
  // scalastyle:on
  /**
   * Returns the metadata for topics. The size of result is always same as the size of topics.
   */
  private def getTopicMetadata(topics: Set[String]): Set[TopicMetadata] = {
    if (topics.isEmpty) {
      return Set.empty
    }
    val req =
      TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    // TopicMetadataRequest can be sent to any broker
    withAnyBroker(Random.shuffle(config.seedBrokers)) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      logDebug(s"TopicMetadataRequest: $req TopicMetadataResponse: $resp")
      // UnknownTopicOrPartitionCode will be returned for an unknown topic, so `resp`'s size
      // should always be same.
      assert(
        resp.topicsMetadata.size == topics.size,
        s"Get unexpected metadata response: $resp")
      // TopicMetadataRequest can be sent to any broker, so we can return once receiving a
      // response from any broker.
      resp.topicsMetadata.toSet
    }
  }

  // Leader offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
  // scalastyle:on

  /**
   * Return the latest offsets of `TopicAndPartition`s. Unknown topics or partitions will be
   * ignored.
   */
  def getLatestOffsets(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] =
    getOffsets(topicAndPartitions, OffsetRequest.LatestTime)

  /**
   * Return the earliest offsets of `TopicAndPartition`s. Unknown topics or partitions will be
   * ignored.
   */
  def getEarliestOffsets(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] =
    getOffsets(topicAndPartitions, OffsetRequest.EarliestTime)

  private def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }

  /**
   * Return the offsets of `TopicAndPartition`s. Unknown topics or partitions will be ignored.
   */
  private def getOffsets(
    topicAndPartitions: Set[TopicAndPartition],
    before: Long): Map[TopicAndPartition, Long] = {
    if (topicAndPartitions.isEmpty) {
      return Map.empty
    }
    val result = mutable.Map[TopicAndPartition, Long]()
    val tpToLeader = findLeaders(topicAndPartitions)
    if (tpToLeader.isEmpty) {
      return Map.empty
    }
    val leaderToTp = flip(tpToLeader)
    val leaders = leaderToTp.keys
    withAllBrokers(leaders) { consumer =>
      val partitions = leaderToTp((consumer.host, consumer.port))
      val reqMap = partitions.map(_ -> PartitionOffsetRequestInfo(before, maxNumOffsets = 1)).toMap
      val req = OffsetRequest(reqMap)
      val resp = consumer.getOffsetsBefore(req)
      logDebug(s"OffsetRequest: $req OffsetResponse: $resp")
      val respMap = resp.partitionErrorAndOffsets
      partitions.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
          if (por.error == ErrorMapping.NoError) {
            // scalastyle:off
            // As per https://github.com/apache/kafka/blob/0.8.2.1/core/src/main/scala/kafka/server/KafkaApis.scala#L451
            // `por.offsets` won't be empty, and since maxNumOffsets is 1, so the size of offsets
            // must be 1.
            // scalastyle:on
            assert(por.offsets.size == 1, s"Received invalid offsets (${por.offsets}) for $tp")
            result += tp -> por.offsets.head
          } else if (por.error == ErrorMapping.UnknownTopicOrPartitionCode) {
            // Ignore this partition
          } else {
            throw ErrorMapping.exceptionFor(por.error)
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return result.toMap
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    if (missing.nonEmpty) {
      logDebug(s"Cannot find offsets for $missing")
    }
    result.toMap
  }

  /**
   * Try a call against potentially multiple brokers. It returns immediately if `fn` succeeds on one
   * broker.
   */
  private def withAnyBroker[T](brokers: Iterable[(String, Int)])(fn: SimpleConsumer => T): T = {
    assert(brokers.nonEmpty, "brokers not specified")
    var lastException: Throwable = null
    brokers.foreach { hp =>
      val consumer = connect(hp._1, hp._2)
      try {
        return fn(consumer)
        // If the operation succeeded, ignore `lastException` from other broker
      } catch {
        case NonFatal(e) =>
          // Use `logDebug` otherwise when some brokers are down, it will output too many logs.
          logDebug(e.getMessage, e)
          lastException = e
      } finally {
        consumer.close()
      }
    }
    // brokers is not empty, so `lastException` must not be null.
    assert(lastException != null)
    // Throw the last exception to the user
    throw lastException
  }

  /**
   * Run `fn` against all specified brokers.
   */
  private def withAllBrokers(brokers: Iterable[(String, Int)])(fn: SimpleConsumer => Unit): Unit = {
    assert(brokers.nonEmpty, "brokers not specified")
    brokers.foreach { hp =>
      val consumer = connect(hp._1, hp._2)
      try {
        fn(consumer)
      } finally {
        consumer.close()
      }
    }
  }
}

object KafkaCluster {

  /**
   * High-level kafka consumers connect to ZK.  ConsumerConfig assumes this use case.
   * Simple consumers connect directly to brokers, but need many of the same configs.
   * This subclass won't warn about missing ZK params, or presence of broker params.
   */
  class SimpleConsumerConfig private(brokers: String, originalProps: Properties)
      extends ConsumerConfig(originalProps) {
    val seedBrokers: Array[(String, Int)] = brokers.split(",").map { hp =>
      val hpa = hp.split(":")
      if (hpa.size == 1) {
        throw new SparkException(s"Broker not in the correct format of <host>:<port> [$brokers]")
      }
      (hpa(0), hpa(1).toInt)
    }
    require(seedBrokers.nonEmpty, "Must specify metadata.broker.list or bootstrap.servers")
  }

  object SimpleConsumerConfig {
    /**
     * Make a consumer config without requiring group.id or zookeeper.connect,
     * since communicating with brokers also needs common settings such as timeout
     */
    def apply(kafkaParams: Map[String, String]): SimpleConsumerConfig = {
      // These keys are from other pre-existing kafka configs for specifying brokers, accept either
      val brokers = kafkaParams.get("bootstrap.servers")
        .orElse(kafkaParams.get("metadata.broker.list"))
        .getOrElse {
          // KafkaSourceProvider already checked the options. This should not happen.
          throw new AssertionError("Must specify metadata.broker.list or bootstrap.servers")
        }

      val props = new Properties()
      kafkaParams.foreach { case (key, value) =>
        // prevent warnings on parameters ConsumerConfig doesn't know about
        if (key != "metadata.broker.list" && key != "bootstrap.servers") {
          props.put(key, value)
        }
      }

      Seq("zookeeper.connect", "group.id").foreach { s =>
        if (!props.containsKey(s)) {
          props.setProperty(s, "")
        }
      }

      new SimpleConsumerConfig(brokers, props)
    }
  }
}

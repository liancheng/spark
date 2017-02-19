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

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.Random

import kafka.common.TopicAndPartition
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.test.SharedSQLContext

/**
 * The original file is `org.apache.spark.sql.kafka010.KafkaSourceSuite`.
 */
abstract class KafkaSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because KafkaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race contion,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q.processAllAvailable()
    true
  }

  /**
   * Add data to Kafka.
   *
   * `topicAction` can be used to run actions for each topic before inserting data.
   */
  case class AddKafkaData(topics: Set[String], data: Int*)
    (implicit ensureDataInMultiplePartition: Boolean = false,
      concurrent: Boolean = false,
      message: String = "",
      topicAction: (String, Option[Int]) => Unit = (_, _) => {}) extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        // Make sure no Spark job is running when deleting a topic
        query.get.processAllAvailable()
      }

      val existingTopics = testUtils.getAllTopicsAndPartitionSize().toMap
      val newTopics = topics.diff(existingTopics.keySet)
      for (newTopic <- newTopics) {
        topicAction(newTopic, None)
      }
      for (existingTopicPartitions <- existingTopics) {
        topicAction(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      // Read all topics again in case some topics are delete.
      val allTopics = testUtils.getAllTopicsAndPartitionSize().toMap.keys
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active kafka source")

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[KafkaSource] =>
          source.asInstanceOf[KafkaSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Kafka source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Kafka source in the StreamExecution logical plan as there" +
            "are multiple Kafka sources:\n\t" + sources.mkString("\n\t"))
      }
      val kafkaSource = sources.head
      val topic = topics.toSeq(Random.nextInt(topics.size))
      val numPartitions = testUtils.getAllTopicsAndPartitionSize().toMap.apply(topic)
      testUtils.sendMessages(topic, data.map { _.toString }.toArray)

      val offset = KafkaSourceOffset(testUtils.getLatestOffsets(topics))

      // Verify that the test data gets inserted into multiple partitions
      if (ensureDataInMultiplePartition) {
        require(
          offset.partitionToOffsets.count(_._2 > 0) === numPartitions,
          "Added data does not test multiple partitions. " +
            s"Current offset: $offset, # Partitions: $numPartitions")
      }
      logInfo(s"Added data, expected offset $offset")
      (kafkaSource, offset)
    }

    override def toString: String =
      s"AddKafkaData(topics = $topics, data = $data, message = $message)"
  }
}


class KafkaSourceSuite extends KafkaSourceTest {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  testWithUninterruptibleThread("deserialization of initial offset with Spark 2.1.0") {
    withTempDir { metadataPath =>
      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 3)

      val provider = new KafkaSourceProvider
      val parameters = Map(
        "kafka.bootstrap.servers" -> testUtils.brokerAddress,
        "subscribe" -> topic
      )
      val source = provider.createSource(spark.sqlContext, metadataPath.getAbsolutePath, None,
        "", parameters)
      source.getOffset.get // Write initial offset

      // Make sure Spark 2.1.0 will throw an exception when reading the new log
      intercept[java.lang.IllegalArgumentException] {
        // Simulate how Spark 2.1.0 reads the log
        val in = new FileInputStream(metadataPath.getAbsolutePath + "/0")
        val length = in.read()
        val bytes = new Array[Byte](length)
        in.read(bytes)
        KafkaSourceOffset(SerializedOffset(new String(bytes, UTF_8)))
      }
    }
  }

  testWithUninterruptibleThread("deserialization of initial offset written by Spark 2.1.0") {
    withTempDir { metadataPath =>
      val topic = "kafka-initial-offset-2-1-0"
      testUtils.createTopic(topic, partitions = 3)

      val provider = new KafkaSourceProvider
      val parameters = Map(
        "kafka.bootstrap.servers" -> testUtils.brokerAddress,
        "subscribe" -> topic
      )

      val from = Paths.get(
        getClass.getResource("/kafka-source-initial-offset-version-2.1.0.bin").getPath)
      val to = Paths.get(s"${metadataPath.getAbsolutePath}/0")
      Files.copy(from, to)

      val source = provider.createSource(spark.sqlContext, metadataPath.getAbsolutePath, None,
        "", parameters)
      val deserializedOffset = source.getOffset.get
      val referenceOffset = KafkaSourceOffset((topic, 0, 0L), (topic, 1, 0L), (topic, 2, 0L))
      assert(referenceOffset == deserializedOffset)
    }
  }

  testWithUninterruptibleThread("deserialization of initial offset written by future version") {
    withTempDir { metadataPath =>
      val futureMetadataLog =
        new HDFSMetadataLog[KafkaSourceOffset](sqlContext.sparkSession,
          metadataPath.getAbsolutePath) {
          override def serialize(metadata: KafkaSourceOffset, out: OutputStream): Unit = {
            out.write(0)
            val writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8))
            writer.write(s"v0\n${metadata.json}")
            writer.flush()
          }
        }

      val topic = newTopic()
      testUtils.createTopic(topic, partitions = 3)
      val offset = KafkaSourceOffset((topic, 0, 0L), (topic, 1, 0L), (topic, 2, 0L))
      futureMetadataLog.add(0, offset)

      val provider = new KafkaSourceProvider
      val parameters = Map(
        "kafka.bootstrap.servers" -> testUtils.brokerAddress,
        "subscribe" -> topic
      )
      val source = provider.createSource(spark.sqlContext, metadataPath.getAbsolutePath, None,
        "", parameters)

      val e = intercept[java.lang.IllegalStateException] {
        source.getOffset.get // Read initial offset
      }

      assert(e.getMessage.contains("Please upgrade your Spark"))
    }
  }

  test("(de)serialization of initial offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 64)

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)

    testStream(reader.load())(
      makeSureGetOffsetCalled,
      StopStream,
      StartStream(),
      StopStream)
  }

  test("maxOffsetsPerTrigger") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("1"), Some(2))

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // 1 from smallest, 1 from middle, 8 from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116
      ),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121, 122, 123, 124, 125
      ),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107,
        11, 108, 109, 110, 111, 112, 113, 114, 115, 116,
        12, 117, 118, 119, 120, 121, 122, 123, 124, 125,
        13, 126, 127, 128, 129, 130, 131, 132, 133, 134
      )
    )
  }

  test("cannot stop Kafka stream") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  test(s"assign from latest offsets") {
    val topic = newTopic()
    testFromLatestOffsets(
      topic,
      addPartitions = false,
      "assign" -> assignString(topic, 0 to 4))
  }

  test(s"assign from earliest offsets") {
    val topic = newTopic()
    testFromEarliestOffsets(
      topic,
      addPartitions = false,
      "assign" -> assignString(topic, 0 to 4))
  }

  test(s"assign from specific offsets") {
    val topic = newTopic()
    testFromSpecificOffsets(
      topic,
      "assign" -> assignString(topic, 0 to 4))
  }

  test(s"subscribing topic by name from latest offsets") {
    val topic = newTopic()
    testFromLatestOffsets(
      topic,
      addPartitions = true,
      "subscribe" -> topic)
  }

  test(s"subscribing topic by name from earliest offsets") {
    val topic = newTopic()
    testFromEarliestOffsets(
      topic,
      addPartitions = true,
      "subscribe" -> topic)
  }

  test(s"subscribing topic by name from specific offsets") {
    val topic = newTopic()
    testFromSpecificOffsets(topic, "subscribe" -> topic)
  }

  test("starting offset is latest by default") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("0"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)

    val kafka = reader.load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    val mapped = kafka.map(_.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(1, 2, 3)  // should not have 0
    )
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka08")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase.contains(m.toLowerCase))
      }
    }

    // No strategy specified
    testBadOptions()("options must be specified", "subscribe", "assign")

    // Multiple strategies specified
    testBadOptions("subscribe" -> "t", "assign" -> """{"a":[0]}""")(
      "only one", "options can be specified")

    testBadOptions("assign" -> "")("no topicpartitions to assign")
    testBadOptions("subscribe" -> "")("no topics to subscribe")
  }

  test("input row metrics") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val kafka = spark
      .readStream
      .format("kafka08")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = kafka.map(kv => kv._2.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => TopicAndPartition(topic, p)))
  }

  private def testFromSpecificOffsets(
      topic: String,
      options: (String, String)*): Unit = {
    val partitionOffsets = Map(
      TopicAndPartition(topic, 0) -> -2L,
      TopicAndPartition(topic, 1) -> -1L,
      TopicAndPartition(topic, 2) -> 0L,
      TopicAndPartition(topic, 3) -> 1L,
      TopicAndPartition(topic, 4) -> 2L
    )
    val startingOffsets = JsonUtils.partitionOffsets(partitionOffsets)

    testUtils.createTopic(topic, partitions = 5)
    // part 0 starts at earliest, these should all be seen
    testUtils.sendMessages(topic, Array(-20, -21, -22).map(_.toString), Some(0))
    // part 1 starts at latest, these should all be skipped
    testUtils.sendMessages(topic, Array(-10, -11, -12).map(_.toString), Some(1))
    // part 2 starts at 0, these should all be seen
    testUtils.sendMessages(topic, Array(0, 1, 2).map(_.toString), Some(2))
    // part 3 starts at 1, first should be skipped
    testUtils.sendMessages(topic, Array(10, 11, 12).map(_.toString), Some(3))
    // part 4 starts at 2, first and second should be skipped
    testUtils.sendMessages(topic, Array(20, 21, 22).map(_.toString), Some(4))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka08")
      .option("startingOffsets", startingOffsets)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22), // Should get the data back on recovery
      AddKafkaData(Set(topic), 30, 31, 32, 33, 34)(ensureDataInMultiplePartition = true),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  test("Kafka column types") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, Array(1).map(_.toString))

    val kafka = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingOffsets", s"earliest")
      .option("subscribe", topic)
      .load()

    val query = kafka
      .writeStream
      .format("memory")
      .outputMode("append")
      .queryName("kafkaColumnTypes")
      .start()
    query.processAllAvailable()
    val rows = spark.table("kafkaColumnTypes").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    assert(row.getAs[Array[Byte]]("key") === null, s"Unexpected results: $row")
    assert(row.getAs[Array[Byte]]("value") === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String]("topic") === topic, s"Unexpected results: $row")
    assert(row.getAs[Int]("partition") === 0, s"Unexpected results: $row")
    assert(row.getAs[Long]("offset") === 0L, s"Unexpected results: $row")
    assert(
      row.getAs[java.sql.Timestamp]("timestamp") ===
        DateTimeUtils.toJavaTimestamp(KafkaSourceRDD.NO_TIMESTAMP),
      s"Unexpected results: $row")
    assert(
      row.getAs[Int]("timestampType") === KafkaSourceRDD.NO_TIMESTAMP_TYPE,
      s"Unexpected results: $row")
    query.stop()
  }

  test("unsupported options") {
    def testUnsupportedOption(key: String, value: String = "someValue"): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("kafka08")
          .option("subscribe", "topic")
          .option("kafka.bootstrap.servers", "somehost")
          .option(key, value)
        reader.load()
      }
      for (msg <- Seq("0.8", "not supported", key)) {
        assert(ex.getMessage.contains(msg))
      }
    }

    testUnsupportedOption("subscribePattern")
    testUnsupportedOption("failOnDataLoss")
  }

  private def testFromLatestOffsets(
      topic: String,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, Array("-1"))
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("kafka08")
      .option("startingOffsets", s"latest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddKafkaData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddKafkaData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddKafkaData(Set(topic), 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) {
          testUtils.addPartitions(topic, 10)
        }
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(
      topic: String,
      addPartitions: Boolean,
      options: (String, String)*): Unit = {
    testUtils.createTopic(topic, partitions = 5)
    testUtils.sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(testUtils.getLatestOffsets(Set(topic)).size === 5)

    val reader = spark.readStream
    reader
      .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
      .option("startingOffsets", s"earliest")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
    options.foreach { case (k, v) => reader.option(k, v) }
    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = kafka.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      AddKafkaData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddKafkaData(Set(topic), 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        if (addPartitions) {
          testUtils.addPartitions(topic, 10)
        }
        true
      },
      AddKafkaData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }
}

object KafkaSourceSuite {
  @volatile var globalTestUtils: KafkaTestUtils = _
  val collectedData = new ConcurrentLinkedQueue[Any]()
}


class KafkaSourceStressSuite extends KafkaSourceTest {

  import testImplicits._

  val topicId = new AtomicInteger(1)

  def newStressTopic: String = s"stress${topicId.getAndIncrement()}"

  private def nextInt(start: Int, end: Int): Int = {
    start + Random.nextInt(start + end - 1)
  }

  test("stress test with multiple topics and partitions")  {
    val topics = mutable.ArrayBuffer.fill(5)(newStressTopic)
    topics.foreach { topic =>
      testUtils.createTopic(topic, partitions = nextInt(1, 6))
      testUtils.sendMessages(topic, (101 to 105).map { _.toString }.toArray)
    }

    val newTopicsToCreate = mutable.ArrayBuffer.fill(10)(newStressTopic)

    // Create Kafka source that reads from latest offset
    val kafka =
      spark.readStream
        .format(classOf[KafkaSourceProvider].getCanonicalName.stripSuffix("$"))
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", (topics ++ newTopicsToCreate).mkString(","))
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]

    val mapped = kafka.map(kv => kv._2.toInt + 1)

    runStressTest(
      mapped,
      Seq(makeSureGetOffsetCalled),
      (d, running) => {
        Random.nextInt(5) match {
          case 0 if newTopicsToCreate.nonEmpty => // Add a new topic
            val nextTopic = newTopicsToCreate.remove(Random.nextInt(newTopicsToCreate.size))
            topics += nextTopic
            AddKafkaData(topics.toSet, d: _*)(message = s"Add topic $newStressTopic",
              topicAction = (topic, partition) => {
                if (partition.isEmpty) {
                  testUtils.createTopic(topic, partitions = nextInt(1, 6))
                }
              })
          case 1 => // Add new partitions
            AddKafkaData(topics.toSet, d: _*)(message = "Add partition",
              topicAction = (topic, partition) => {
                testUtils.addPartitions(topic, partition.get + nextInt(1, 6))
              })
          case _ => // Just add new data
            AddKafkaData(topics.toSet, d: _*)
        }
      },
      iterations = 50)
  }
}

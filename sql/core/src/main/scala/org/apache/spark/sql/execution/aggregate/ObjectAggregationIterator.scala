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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.ObjectAggregateMap
import org.apache.spark.sql.execution.ObjectAggregateMap.AggregationBufferInitializer
import org.apache.spark.unsafe.KVIterator

class ObjectAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): MutableRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: MutableRow = new SpecificMutableRow(bufferSchema.map(_.dataType))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to generate output rows.
  override protected def generateResultProjection(): (InternalRow, MutableRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val joinedRow = new JoinedRow()
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ aggregateAttributes,
        groupingAttributes ++ aggregateAttributes)

      (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else {
      super.generateResultProjection()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  private[this] val hashMap = new ObjectAggregateMap(new AggregationBufferInitializer() {
    override def initialize(): MutableRow = createNewAggregationBuffer()
  })

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  private def processInputs(): Unit = {
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)
      val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(buffer, newInput)
      }
    } else {
      var i = 0
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        val groupingKey = groupingProjection.apply(newInput).copy()
        val buffer: MutableRow = hashMap.getAggregationBufferByKey(groupingKey)
        processRow(buffer, newInput)
        i += 1
      }
    }

    val iterator = hashMap.iterator()
    while (iterator.next()) {
      aggregateFunctions.foreach {
        case agg: ObjectAggregateFunction => agg.serializeAggregateBuffer(iterator.getValue)
        case _ =>
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  private[this] var aggregationBufferMapIterator: KVIterator[InternalRow, MutableRow] = _

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.
   */
  processInputs()

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) {
      hashMap.free()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    mapIteratorHasNext
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = {
        // We did not fall back to sort-based aggregation.
        val result = generateOutput(
          aggregationBufferMapIterator.getKey,
          aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      val defaultAggregationBuffer = createNewAggregationBuffer()
      // We create an output row and copy it. So, we can free the map.
      val resultCopy = generateOutput(
        UnsafeRow.createFromByteArray(0, 0), defaultAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}

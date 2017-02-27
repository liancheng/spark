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

package org.apache.spark.sql.execution.joins

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  /** some join types do not yet have codegen */
  override val supportCodegen: Boolean = (joinType, buildSide) match {
    case (_: InnerLike, _) => true
    case (LeftSemi | LeftAnti, BuildRight) => true
    case _ => false
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(j) =>
      UnsafeProjection.create(output, output)
    case other =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      newPredicate(condition.get, streamed.output ++ broadcast.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  /**
   * The implementation for InnerJoin.
   */
  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  private def outerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        private var foundMatch: Boolean = false
        // the matched result row
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        private var nextIndex: Int = 0

        private def findNextMatch(): Boolean = {
          if (streamRow == null) {
            if (!streamedIter.hasNext) {
              return false
            }
            streamRow = streamedIter.next()
            nextIndex = 0
            foundMatch = false
          }
          while (nextIndex < buildRows.length) {
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            nextIndex += 1
            if (boundCondition(resultRow)) {
              foundMatch = true
              return true
            }
          }
          if (!foundMatch) {
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext(): Boolean = {
          resultRow != null || findNextMatch()
        }
        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftSemi with BuildRight
   *   Anti with BuildRight
   */
  private def leftExistenceJoin(
      relation: Broadcast[Array[InternalRow]],
      exists: Boolean): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) {
        streamedIter.filter(l =>
          buildRows.exists(r => boundCondition(joinedRow(l, r))) == exists
        )
      } else if (buildRows.nonEmpty == exists) {
        streamedIter
      } else {
        Iterator.empty
      }
    }
  }

  private def existenceJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) {
        val resultRow = new GenericInternalRow(Array[Any](null))
        streamedIter.map { row =>
          val result = buildRows.exists(r => boundCondition(joinedRow(row, r)))
          resultRow.setBoolean(0, result)
          joinedRow(row, resultRow)
        }
      } else {
        val resultRow = new GenericInternalRow(Array[Any](buildRows.nonEmpty))
        streamedIter.map { row =>
          joinedRow(row, resultRow)
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildLeft
   *   RightOuter with BuildRight
   *   FullOuter
   *   LeftSemi with BuildLeft
   *   LeftAnti with BuildLeft
   *   ExistenceJoin with BuildLeft
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    /** All rows that either match both-way, or rows from streamed joined with nulls. */
    val streamRdd = streamed.execute()

    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matched.set(i)
          }
          i += 1
        }
      }
      Seq(matched).toIterator
    }

    val matchedBroadcastRows = matchedBuildRows.fold(
      new BitSet(relation.value.length)
    )(_ | _)

    joinType match {
      case LeftSemi =>
        assert(buildSide == BuildLeft)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (matchedBroadcastRows.get(i)) {
            buf += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case j: ExistenceJoin =>
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          val result = new GenericInternalRow(Array[Any](matchedBroadcastRows.get(i)))
          buf += new JoinedRow(rel(i).copy(), result)
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case LeftAnti =>
        val notMatched: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (!matchedBroadcastRows.get(i)) {
            notMatched += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(notMatched)
      case o =>
    }

    val notMatchedBroadcastRows: Seq[InternalRow] = {
      val nulls = new GenericInternalRow(streamed.output.size)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      val joinedRow = new JoinedRow
      joinedRow.withLeft(nulls)
      var i = 0
      val buildRows = relation.value
      while (i < buildRows.length) {
        if (!matchedBroadcastRows.get(i)) {
          buf += joinedRow.withRight(buildRows(i)).copy()
        }
        i += 1
      }
      buf
    }

    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
          i += 1
        }

        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    sparkContext.union(
      matchedStreamRows,
      sparkContext.makeRDD(notMatchedBroadcastRows)
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation)
      case (LeftSemi, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = true)
      case (LeftAnti, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = false)
      case (j: ExistenceJoin, BuildRight) =>
        existenceJoin(broadcastedRelation)
      case _ =>
        /**
         * LeftOuter with BuildLeft
         * RightOuter with BuildRight
         * FullOuter
         * LeftSemi with BuildLeft
         * LeftAnti with BuildLeft
         * ExistenceJoin with BuildLeft
         */
        defaultJoin(broadcastedRelation)
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  override def inputRDDs() : Seq[RDD[InternalRow]] = {
    streamed.asInstanceOf[CodegenSupport].inputRDDs()
  }

  /**
   * Returns code that evaluates the join condition and executes continue in the loop
   * if the join condition is false.
   * @param ctx the [[CodegenContext]]
   * @param streamVars variables for the streamed side
   * @param broadcastVars variables for the broadcast side
   * @return code for the join condition
   */
  def getJoinConditionCode(
      ctx: CodegenContext,
      streamVars: Seq[ExprCode],
      broadcastVars: Seq[ExprCode]): String = {
    if (condition.isDefined) {
      val vars = streamVars ++ broadcastVars
      ctx.currentVars = vars
      val outputs = streamed.output ++ broadcast.output
      val eval = evaluateRequiredVariables(outputs, vars, condition.get.references)
      val be = BindReferences.bindReference(condition.get, outputs).genCode(ctx)
      s"""
         |/* evaluate required variables  */
         |$eval
         |/* evaluate join condition */
         |${be.code}
         |/* test join condition */
         |if (${be.isNull} || !(${be.value})) continue;
       """.stripMargin
    } else {
      ""
     }
  }

  /**
   * Generate code for the broadcast side variables.
   * @param ctx the [[CodegenContext]]
   * @param broadcastRow the variable holding the broadcast (i.e. build side) row
   * @return Seq of ExprCode for accessing the broadcast row columns
   */
  def genBroadcastVars(ctx: CodegenContext, broadcastRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = broadcastRow
    ctx.currentVars = null
    broadcast.output.zipWithIndex.map { case (e, i) =>
      BoundReference(i, e.dataType, e.nullable).genCode(ctx)
    }
  }

  /**
   * Codegen equivalent of leftExistenceJoin method. Generates code for these joins:
   * LeftSemi with BuildRight, Anti with BuildRight.
   * @param ctx the [[CodegenContext]]
   * @param broadcastRow name of the variable holding the broadcast row
   * @param broadcastRelationVar name of the variable holding the broadcast relation
   * @param broadcastVars Seq of gen codes for the broadcast row variables
   * @param streamVars Seq of gen codes for the streamed row variables
   * @param exists false for anti join, true otherwise
   * @return code for join
   */
  def codegenLeftExistence(
      ctx: CodegenContext,
      broadcastRow: String,
      broadcastRelationVar: String,
      broadcastVars: Seq[ExprCode],
      streamVars: Seq[ExprCode],
      exists: Boolean): String = {
    val foundMatch = ctx.freshName("foundMatch")
    val joinConditionSkip = getJoinConditionCode(ctx, streamVars, broadcastVars)
    val index = ctx.freshName("index")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val existsVal = if (exists) {
      "true"
    } else {
      "false"
    }
    s"""
       |boolean $foundMatch = false;
       |for(int $index = 0; $index < ${broadcastRelationVar}.length && !$foundMatch; ++$index) {
       |  InternalRow $broadcastRow = ${broadcastRelationVar}[$index];
       |  $joinConditionSkip
       |  $foundMatch = true;
       |}
       |if ($foundMatch == $existsVal) {
       |  $numOutput.add(1);
       |  ${consume(ctx, streamVars)}
       |}
     """.stripMargin
  }

  /**
   * Codegen equivalent of innerJoin method. Generates code for InnerJoin.
   * @param ctx the [[CodegenContext]]
   * @param broadcastRow name of the variable holding the broadcast row
   * @param broadcastRelationVar name of the variable holding the broadcast relation
   * @param broadcastVars Seq of gen codes for the broadcast row variables
   * @param streamVars Seq of gen codes for the streamed row variables
   * @return code for join
   */
  def codegenInner(
      ctx: CodegenContext,
      broadcastRow: String,
      broadcastRelationVar: String,
      broadcastVars: Seq[ExprCode],
      streamVars: Seq[ExprCode]): String = {
    val joinConditionSkip = getJoinConditionCode(ctx, streamVars, broadcastVars)
    val index = ctx.freshName("index")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val vars = buildSide match {
      case BuildLeft => broadcastVars ++ streamVars
      case BuildRight => streamVars ++ broadcastVars
    }

    s"""
       |for(int $index = 0; $index < ${broadcastRelationVar}.length; ++$index) {
       |  InternalRow $broadcastRow = ${broadcastRelationVar}[$index];
       |  $joinConditionSkip
       |  $numOutput.add(1);
       |  ${consume(ctx, vars)}
       |}
     """.stripMargin
  }

  /**
   * Currently there are equivalent codegen methods for the following existing methods:
   * innerJoin, leftExistenceJoin.
   * Not yet supported: outerJoin, existenceJoin, defaultJoin.
   */
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val broadcastRelation = broadcast.doExecuteBroadcast[Array[InternalRow]]()
    val broadcastRelationVar = ctx.addReferenceObj("broadcast", broadcastRelation.value,
      "InternalRow[]")
    val broadcastRow = ctx.freshName("broadcastRow")
    val broadcastVars = genBroadcastVars(ctx, broadcastRow)

    ctx.copyResult = true
    joinType match {
      case _: InnerLike =>
        codegenInner(ctx, broadcastRow, broadcastRelationVar, broadcastVars, input)
      case LeftSemi =>
        codegenLeftExistence(ctx, broadcastRow, broadcastRelationVar, broadcastVars, input, true)
      case LeftAnti =>
        codegenLeftExistence(ctx, broadcastRow, broadcastRelationVar, broadcastVars, input, false)
      case x =>
        sys.error(s"BroadcastNestedLoopJoin codegen does not support $x as the JoinType")
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamed.asInstanceOf[CodegenSupport].produce(ctx, this)
  }
}

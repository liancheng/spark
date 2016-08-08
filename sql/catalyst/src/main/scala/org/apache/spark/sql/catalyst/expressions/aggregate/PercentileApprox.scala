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

package org.apache.spark.sql.catalyst.expressions.aggregate

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.QuantileSummaries.Stats
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._

/**
 * Computes an approximate percentile (quantile) using the G-K algorithm (see below), for very
 * large numbers of rows where the regular percentile() UDAF might run out of memory.
 *
 * The input is a single double value or an array of double values representing the percentiles
 * requested. The output, corresponding to the input, is either a single double value or an
 * array of doubles that are the percentile values.
 */
@ExpressionDescription(
  usage = """_FUNC_(col, p [, B]) - Returns an approximate pth percentile of a numeric column in the
     group. The B parameter, which defaults to 1000, controls approximation accuracy at the cost of
     memory; higher values yield better approximations.
    _FUNC_(col, array(p1 [, p2]...) [, B]) - Same as above, but accepts and returns an array of
     percentile values instead of a single one.
    """)
case class PercentileApprox(
    child: Expression,
    percentiles: Seq[Double],
    B: Int,
    resultAsArray: Boolean,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends ImperativeAggregate with ObjectAggregateFunction {

  if (!resultAsArray) assert(percentiles.length == 1)

  // Constructor for the "_FUNC_(col, p, B) / _FUNC_(col, array(p1, ...), B)" form
  def this(child: Expression, percentilesExpr: Expression, bExpr: Expression) = {
    this(
      child = child,
      percentiles = PercentileApprox.validatePercentilesExpression(percentilesExpr),
      B = PercentileApprox.validateBExpression(bExpr),
      resultAsArray = percentilesExpr.dataType.isInstanceOf[ArrayType])
  }

  // Constructor for the "_FUNC_(col, p) / _FUNC_(col, array(p1, ...))" form
  def this(child: Expression, percentilesExpr: Expression) = {
    this(child, percentilesExpr, Literal(1000))
  }

  override def prettyName: String = "percentile_approx"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  // we would return null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = if (resultAsArray) ArrayType(DoubleType) else DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def supportsPartial: Boolean = true

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val aggBufferAttributes: Seq[AttributeReference] =
    Seq(AttributeReference("buf", BinaryType)())

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def initialize(buffer: MutableRow): Unit = {
    // Our `PercentileApprox` function takes a `B` parameter, but the underlying GK algorithm
    // takes a `relativeError` parameter, so we need to convert `B` to `relativeError`.
    // Please refer to SPARK-16283 for details.
    val summaries = QuantileSummaries(Math.max(1.0d / B, 0.001))
    buffer.update(mutableAggBufferOffset, summaries)
  }

  private def getBufferObject(buffer: MutableRow): QuantileSummaries = {
    buffer.get(mutableAggBufferOffset, null).asInstanceOf[QuantileSummaries]
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val value = child.eval(input) match {
      case o: Byte => o.toDouble
      case o: Short => o.toDouble
      case o: Int => o.toDouble
      case o: Long => o.toDouble
      case o: Float => o.toDouble
      case o: Decimal => o.toDouble
      case o: Double => o
    }
    buffer(mutableAggBufferOffset) = getBufferObject(buffer).insert(value)
  }

  private lazy val encoder = ExpressionEncoder[QuantileSummaries].resolveAndBind()
  private lazy val row = new UnsafeRow(1)

  def serializeAggregateBuffer(buffer: MutableRow): Unit = {
    buffer(mutableAggBufferOffset) =
      encoder.toRow(getBufferObject(buffer).compress()).asInstanceOf[UnsafeRow].getBytes
  }

  override def merge(buffer: MutableRow, inputBuffer: InternalRow): Unit = {
    val bytes = inputBuffer.getBinary(inputAggBufferOffset)
    row.pointTo(bytes, bytes.length)
    buffer(mutableAggBufferOffset) = getBufferObject(buffer).merge(encoder.fromRow(row))
  }

  override def eval(buffer: InternalRow): Any = {
    val bytes = buffer.getBinary(inputAggBufferOffset)
    row.pointTo(bytes, bytes.length)
    val summaries = encoder.fromRow(row)

    if (summaries.count == 0) {
      // return null for empty inputs
      null
    } else if (resultAsArray) {
      // return the result as an array of doubles
      new GenericArrayData(percentiles.map(summaries.query))
    } else {
      // return the result as a double
      summaries.query(percentiles.head)
    }
  }

  private def childrenSQL: String = {
    val percentileString = if (resultAsArray) {
      percentiles.mkString("array(", ",", ")")
    } else {
      percentiles.head.toString
    }

    s"${child.sql}, $percentileString, $B"
  }

  override def sql: String = s"$prettyName($childrenSQL)"

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct$childrenSQL)"
  }
}

object PercentileApprox {
  /**
   * Validates the percentile(s) expression and extracts the percentile(s).
   */
  private def validatePercentilesExpression(expr: Expression): Seq[Double] = {
    def withRangeCheck(v: Double): Double = {
      if (0.0 <= v && v <= 1.0) {
        v
      } else {
        throw new AnalysisException("the percentile(s) must be >= 0 and <= 1.0")
      }
    }

    if (!expr.resolved || !expr.foldable) {
      throw new AnalysisException(
        "The percentile(s) argument must be a double or double array literal.")
    }

    expr.eval() match {
      case f: Float => Seq(withRangeCheck(f.toDouble))
      case d: Double => Seq(withRangeCheck(d))
      case d: Decimal => Seq(withRangeCheck(d.toDouble))
      case array: ArrayData if array.numElements() > 0 =>
        expr.dataType.asInstanceOf[ArrayType].elementType match {
          case FloatType => array.toFloatArray().map(f => withRangeCheck(f.toDouble))
          case DoubleType => array.toDoubleArray().map(withRangeCheck)
          case d: DecimalType => array.toArray[Decimal](d).map(d => withRangeCheck(d.toDouble))
        }
      case _ =>
        throw new AnalysisException(
          "The percentile(s) argument must be a double or double array literal.")
    }
  }

  /** Validates the B expression and extracts its value. */
  private def validateBExpression(exp: Expression): Int = exp match {
    case Literal(i: Int, IntegerType) if i > 0 => i
    case _ =>
      throw new AnalysisException("The B argument must be a positive integer literal")
  }
}

/**
 * Helper class to compute approximate quantile summary.
 * This implementation is based on the algorithm proposed in the paper:
 * "Space-efficient Online Computation of Quantile Summaries" by Greenwald, Michael
 * and Khanna, Sanjeev. (http://dx.doi.org/10.1145/375663.375670)
 *
 * In order to optimize for speed, it maintains an internal buffer of the last seen samples,
 * and only inserts them after crossing a certain size threshold. This guarantees a near-constant
 * runtime complexity compared to the original algorithm.
 *
 * @param relativeError the target relative error.
 *   It is uniform across the complete range of values.
 * @param sampled a buffer of quantile statistics.
 *   See the G-K article for more details.
 * @param count the count of all the elements *inserted in the sampled buffer*
 *              (excluding the head buffer)
 */
case class QuantileSummaries(
    relativeError: Double,
    sampled: Array[Stats] = Array.empty,
    count: Long = 0L) extends Serializable {

  import QuantileSummaries._

  // a buffer of latest samples seen so far
  private val headSampled: ArrayBuffer[Double] = ArrayBuffer.empty

  /**
   * Returns a summary with the given observation inserted into the summary.
   * This method may either modify in place the current summary (and return the same summary,
   * modified in place), or it may create a new summary from scratch it necessary.
   * @param x the new observation to insert into the summary
   */
  def insert(x: Double): QuantileSummaries = {
    headSampled.append(x)
    if (headSampled.size >= defaultHeadSize) {
      this.withHeadBufferInserted
    } else {
      this
    }
  }

  /**
   * Inserts an array of (unsorted samples) in a batch, sorting the array first to traverse
   * the summary statistics in a single batch.
   *
   * This method does not modify the current object and returns if necessary a new copy.
   *
   * @return a new quantile summary object.
   */
  private def withHeadBufferInserted: QuantileSummaries = {
    if (headSampled.isEmpty) {
      return this
    }
    var currentCount = count
    val sorted = headSampled.toArray.sorted
    val newSamples: ArrayBuffer[Stats] = new ArrayBuffer[Stats]()
    // The index of the next element to insert
    var sampleIdx = 0
    // The index of the sample currently being inserted.
    var opsIdx: Int = 0
    while(opsIdx < sorted.length) {
      val currentSample = sorted(opsIdx)
      // Add all the samples before the next observation.
      while(sampleIdx < sampled.size && sampled(sampleIdx).value <= currentSample) {
        newSamples.append(sampled(sampleIdx))
        sampleIdx += 1
      }

      // If it is the first one to insert, of if it is the last one
      currentCount += 1
      val delta =
        if (newSamples.isEmpty || (sampleIdx == sampled.size && opsIdx == sorted.length - 1)) {
          0
        } else {
          math.floor(2 * relativeError * currentCount).toInt
        }

      val tuple = Stats(currentSample, 1, delta)
      newSamples.append(tuple)
      opsIdx += 1
    }

    // Add all the remaining existing samples
    while(sampleIdx < sampled.size) {
      newSamples.append(sampled(sampleIdx))
      sampleIdx += 1
    }
    QuantileSummaries(relativeError, newSamples.toArray, currentCount)
  }

  /**
   * Returns a new summary that compresses the summary statistics and the head buffer.
   *
   * This implements the COMPRESS function of the GK algorithm. It does not modify the object.
   *
   * @return a new summary object with compressed statistics
   */
  def compress(): QuantileSummaries = {
    // Inserts all the elements first
    val inserted = this.withHeadBufferInserted
    assert(inserted.headSampled.isEmpty)
    assert(inserted.count == count + headSampled.size)
    val compressed =
      compressImmut(inserted.sampled, mergeThreshold = 2 * relativeError * inserted.count)
    QuantileSummaries(relativeError, compressed, inserted.count)
  }

  /**
   * Merges two (compressed) summaries together.
   *
   * Returns a new summary.
   */
  def merge(other: QuantileSummaries): QuantileSummaries = {
    require(headSampled.isEmpty, "Current buffer needs to be compressed before merge")
    require(other.headSampled.isEmpty, "Other buffer needs to be compressed before merge")
    if (other.count == 0) {
      this.copy()
    } else if (count == 0) {
      other.copy()
    } else {
      // Merge the two buffers.
      // The GK algorithm is a bit unclear about it, but it seems there is no need to adjust the
      // statistics during the merging: the invariants are still respected after the merge.
      // TODO: could replace full sort by ordered merge, the two lists are known to be sorted
      // already.
      val res = (sampled ++ other.sampled).sortBy(_.value)
      val comp = compressImmut(res, mergeThreshold = 2 * relativeError * count)
      QuantileSummaries(other.relativeError, comp, other.count + count)
    }
  }

  /**
   * Runs a query for a given quantile.
   * The result follows the approximation guarantees detailed above.
   * The query can only be run on a compressed summary: you need to call compress() before using
   * it.
   *
   * @param quantile the target quantile
   * @return
   */
  def query(quantile: Double): Double = {
    require(quantile >= 0 && quantile <= 1.0, "quantile should be in the range [0.0, 1.0]")
    require(headSampled.isEmpty,
      "Cannot operate on an uncompressed summary, call compress() first")

    if (quantile <= relativeError) {
      return sampled.head.value
    }

    if (quantile >= 1 - relativeError) {
      return sampled.last.value
    }

    // Target rank
    val rank = math.ceil(quantile * count).toInt
    val targetError = math.ceil(relativeError * count)
    // Minimum rank at current sample
    var minRank = 0
    var i = 1
    while (i < sampled.size - 1) {
      val curSample = sampled(i)
      minRank += curSample.g
      val maxRank = minRank + curSample.delta
      if (maxRank - targetError <= rank && rank <= minRank + targetError) {
        return curSample.value
      }
      i += 1
    }
    sampled.last.value
  }
}

object QuantileSummaries {
  // TODO(tjhunter) more tuning could be done one the constants here, but for now
  // the main cost of the algorithm is accessing the data in SQL.
  /**
   * The size of the head buffer.
   */
  val defaultHeadSize: Int = 50000

  /**
   * Statistics from the Greenwald-Khanna paper.
   * @param value the sampled value
   * @param g the minimum rank jump from the previous value's minimum rank
   * @param delta the maximum span of the rank.
   */
  case class Stats(value: Double, g: Int, delta: Int)

  private def compressImmut(
      currentSamples: IndexedSeq[Stats],
      mergeThreshold: Double): Array[Stats] = {
    val res: ArrayBuffer[Stats] = ArrayBuffer.empty
    if (currentSamples.isEmpty) {
      return res.toArray
    }
    // Start for the last element, which is always part of the set.
    // The head contains the current new head, that may be merged with the current element.
    var head = currentSamples.last
    var i = currentSamples.size - 2
    // Do not compress the last element
    while (i >= 1) {
      // The current sample:
      val sample1 = currentSamples(i)
      // Do we need to compress?
      if (sample1.g + head.g + head.delta < mergeThreshold) {
        // Do not insert yet, just merge the current element into the head.
        head = head.copy(g = head.g + sample1.g)
      } else {
        // Prepend the current head, and keep the current sample as target for merging.
        res.prepend(head)
        head = sample1
      }
      i -= 1
    }
    res.prepend(head)
    // If necessary, add the minimum element:
    res.prepend(currentSamples.head)
    res.toArray
  }
}

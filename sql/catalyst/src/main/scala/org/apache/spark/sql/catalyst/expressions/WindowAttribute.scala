package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute


/**
 *
 * @param child the computation being performed
 * @param windowSpec the window spec definition
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 */
case class WindowAttribute(child: Expression, name: String, windowSpec: WindowSpec)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends NamedExpression with trees.UnaryNode[Expression] {

  override type EvaluatedType = Any

  override def eval(input: Row) = child.eval(input)

  override def dataType = child.dataType
  override def nullable = child.nullable

  override def toAttribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child $windowSpec AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifiers :: Nil

}

case class WindowSpec(windowPartition: WindowPartition, windowFrame: WindowFrame)

case class WindowPartition(partitionBy: Seq[Expression], sortBy: Seq[SortOrder])

case class WindowFrame(frameType:String, preceding: Int, following: Int)
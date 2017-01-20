/* Copyright (C) 2016 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.sql.catalyst.expressions.closure

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.xbean.asm5.Type
import org.apache.xbean.asm5.Type._

import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ObjectType, ShortType, StringType}

/**
 * TypeOps Extends ASM Type class to provides some handy methods to make it easier to
 * inter-operate with Java type and Spark-Sql data type .
 *
 * Asm type is defined by ASM library at
 * http://asm.ow2.org/asm50/javadoc/user/org/objectweb/asm/Type.html.
 *
 * Each ASM Type can be mapped to:
 * 1. A Java runtime class, for example, asm type BooleanType can be mapped to java runtime class
 *  classOf[Boolean]
 * 2. A Spark-Sql DataType of [[org.apache.spark.sql.types.DataType]].
 */
class TypeOps(asmType: Type) {
  import TypeOps._

  if (asmType == VOID_TYPE || asmType.getSort == METHOD || asmType.getSort == ARRAY) {
    throw new UnSupportedTypeException(asmType)
  }

  // Run time class. For example ASM BOOLEAN_TYPE has runtime class of Boolean
  def runtimeClass: Class[_] = {
      primitiveTypeInfos.get(asmType).map(_.runtimeClass).getOrElse(loadClass(asmType.getClassName))
  }

  def runtimeClassTag: ClassTag[_] = {
    primitiveTypeInfos.get(asmType).map(_.runtimeClassTag).getOrElse {
      ClassTag(runtimeClass)
    }
  }

  // Whether this type maps to a Java primitive data type.
  // @See http://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
  def isPrimitiveType: Boolean = primitiveTypeInfos.contains(asmType)

  // Whether the runtime class is java.lang.String
  def isStringType: Boolean = asmType == Type.getType(classOf[String])

  // Whether the type is String, Null, or primitive types
  def isAtomicType: Boolean = {
    isStringType || isNullType || primitiveTypeInfos.contains(asmType) ||
      boxTypeInfos.contains(asmType)
  }

  // Whether the runtime class is a Java boxing wrapper class defined at
  // https://docs.oracle.com/javase/tutorial/java/data/autoboxing.html
  def isBoxType: Boolean = boxTypeInfos.contains(asmType)

  // Get the ASM Type of boxing wrapper class.
  def boxType: Option[Type] = {
    asmType match {
      case BOOLEAN_TYPE => Some(Type.getType(classOf[java.lang.Boolean]))
      case BYTE_TYPE => Some(Type.getType(classOf[java.lang.Byte]))
      case CHAR_TYPE => Some(Type.getType(classOf[java.lang.Character]))
      case FLOAT_TYPE => Some(Type.getType(classOf[java.lang.Float]))
      case DOUBLE_TYPE => Some(Type.getType(classOf[java.lang.Double]))
      case INT_TYPE => Some(Type.getType(classOf[java.lang.Integer]))
      case LONG_TYPE => Some(Type.getType(classOf[java.lang.Long]))
      case SHORT_TYPE => Some(Type.getType(classOf[java.lang.Short]))
      case _ => None
    }
  }

  // Whether the runtime class maps to the boxing wrapper class of 'other' type's runtime class.
  def isBoxTypeOf(other: TypeOps): Boolean = {
    !isPrimitiveType && other.isPrimitiveType && Some(asmType) == other.boxType
  }

  // Get the spark-Sql DataType of this type.
  // @See [[org.apache.spark.sql.types.DataType]]
  def sqlType: DataType = asmType match {
    case _ if isStringType => StringType
    case _ if isNullType => NullType
    case _ if primitiveTypeInfos.contains(asmType) => primitiveTypeInfos(asmType).sqlType
    case _ if boxTypeInfos.contains(asmType) => boxTypeInfos(asmType).sqlType
    case _ => ObjectType(loadClass(asmType.getClassName))
  }

  // Whether the runtime class is classOf[Null]
  def isNullType: Boolean = runtimeClass == classOf[Null]

  // Whether the runtime class is a Product class.
  def isProductType: Boolean = classOf[Product].isAssignableFrom(runtimeClass)

  // Whether the runtime class of this type is assignable from runtime class of 'other' type.
  def isAssignableFrom(other: TypeOps): Boolean = runtimeClass.isAssignableFrom(other.runtimeClass)


  private def loadClass(className: String): Class[_] = {
    Thread.currentThread().getContextClassLoader.loadClass(className)
  }
}

object TypeOps {

  implicit def typeToTypeOps(dataType: Type): TypeOps = new TypeOps(dataType)

  class UnSupportedTypeException(dataType: Type)
    extends ClosureTranslationException(dataType.toString)

  // Each TypeInfo has properties like:
  // 1. A Spark-Sql Data Type.
  // 2. A runtime class
  private class TypeInfo(val sqlType: DataType, val runtimeClassTag: ClassTag[_]) {
    def runtimeClass: Class[_] = runtimeClassTag.runtimeClass
  }

  // TypeInfo mapping for Java primitive types.
  private val primitiveTypeInfos = Map(
    BOOLEAN_TYPE -> new TypeInfo(BooleanType, classTag[Boolean]),
    BYTE_TYPE -> new TypeInfo(ByteType, classTag[Byte]),
    // Spark Sql doesn't have CharType. Char is represented as IntegerType.
    CHAR_TYPE -> new TypeInfo(IntegerType, classTag[Char]),
    SHORT_TYPE -> new TypeInfo(ShortType, classTag[Short]),
    INT_TYPE -> new TypeInfo(IntegerType, classTag[Int]),
    LONG_TYPE -> new TypeInfo(LongType, classTag[Long]),
    FLOAT_TYPE -> new TypeInfo(FloatType, classTag[Float]),
    DOUBLE_TYPE -> new TypeInfo(DoubleType, classTag[Double])
  )

  // Type Info mapping for Java boxing wrapper classes.
  private val boxTypeInfos = Map(
    Type.getType(classOf[java.lang.Boolean]) ->
      new TypeInfo(BooleanType, classTag[java.lang.Boolean]),
    // Spark sql doesn't have CharType. Char is represented as IntegerType.
    Type.getType(classOf[java.lang.Character]) ->
      new TypeInfo(IntegerType, classTag[java.lang.Character]),
    Type.getType(classOf[java.lang.Byte]) -> new TypeInfo(ByteType, classTag[java.lang.Byte]),
    Type.getType(classOf[java.lang.Float]) -> new TypeInfo(FloatType, classTag[java.lang.Float]),
    Type.getType(classOf[java.lang.Double]) -> new TypeInfo(DoubleType, classTag[java.lang.Double]),
    Type.getType(classOf[java.lang.Integer]) ->
      new TypeInfo(IntegerType, classTag[java.lang.Integer]),
    Type.getType(classOf[java.lang.Long]) -> new TypeInfo(LongType, classTag[java.lang.Long]),
    Type.getType(classOf[java.lang.Short]) -> new TypeInfo(ShortType, classTag[java.lang.Short])
  )

  // Gets ASM Type list which maps to Java primitive type list defined at
  // http://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
  def primitiveTypes: List[Type] = primitiveTypeInfos.keys.toList.sortBy(_.getSort)
}

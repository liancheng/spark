package org.apache.spark.sql.hive

import com.google.common.base.Objects
import org.apache.hadoop.hive.ql.io.orc.OrcSerde
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructField => HiveField, StructObjectInspector}
import org.apache.hadoop.io.Writable

import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, CodeFormatter, CodeGenerator, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.types.{Decimal, DecimalType, DataType}

private[hive] class FieldWrapper(val dataType: DataType, val field: HiveField) {
  override def equals(obj: Any): Boolean = obj match {
    case that: FieldWrapper =>
      this.dataType == that.dataType &&
        this.field.getFieldName == that.field.getFieldName &&
        this.fieldObjectInspector.getClass == that.fieldObjectInspector.getClass
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(dataType, field.getFieldName, fieldObjectInspector.getClass)

  def fieldObjectInspector: ObjectInspector = field.getFieldObjectInspector
}

private[hive] abstract class UnsafeHiveIterator extends Iterator[UnsafeRow] {
  def initialize(
      writables: Iterator[Writable],
      fieldWrappers: Array[FieldWrapper],
      structOI: StructObjectInspector): Unit
}

object GenerateUnsafeHiveIterator extends CodeGenerator[Seq[FieldWrapper], UnsafeHiveIterator] {
  override protected def canonicalize(in: Seq[FieldWrapper]): Seq[FieldWrapper] = in

  override protected def bind(
      in: Seq[FieldWrapper], inputSchema: Seq[Attribute]): Seq[FieldWrapper] = in

  override protected def create(fieldWrappers: Seq[FieldWrapper]): UnsafeHiveIterator = {
    val context = newCodeGenContext()

    val writeToUnsafeRow = fieldWrappers.zipWithIndex.map { case (fieldWrapper, ordinal) =>
      def append(writeValue: String): String = {
        s"""{
           |  StructField fieldRef = fieldWrappers[$ordinal].field();
           |  Object fieldValue = structOI.getStructFieldData(raw, fieldRef);
           |  $writeValue
           |}
           """.stripMargin
      }

      def appendNullSafe(writeValue: String): String = {
        append(
          s"""  if (fieldValue == null) {
             |    unsafeRowWriter.setNullAt($ordinal);
             |  } else {
             |    $writeValue
             |  }
           """.stripMargin)
      }

      def appendNullSafeSimpleData[T <: ObjectInspector](oiClass: Class[T]): String = {
        val oiClassName = oiClass.getName
        appendNullSafe(
          s"""    $oiClassName oi = ($oiClassName) fieldRef.getFieldObjectInspector();
             |    unsafeRowWriter.write($ordinal, oi.get(fieldValue);
           """.stripMargin)
      }

      (fieldWrapper.fieldObjectInspector, fieldWrapper.dataType) match {
        case (_: BooleanObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[BooleanObjectInspector])

        case (_: ByteObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[ByteObjectInspector])

        case (_: ShortObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[ShortObjectInspector])

        case (_: IntObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[IntObjectInspector])

        case (_: IntObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[IntObjectInspector])

        case (_: LongObjectInspector, _) =>
          appendNullSafeSimpleData(classOf[IntObjectInspector])

        case (_: HiveDecimalObjectInspector, DecimalType.Fixed(p, s))
          if p > Decimal.MAX_LONG_DIGITS =>
          append(
            s"""  if (fieldValue == null) {
               |    unsafeRowWriter.write($ordinal, (Decimal) null, $p, $s);
               |  } else {
               |    unsafeRowWriter.write($ordinal, (Decimal) null, $p, $s);
               |  }
             """.stripMargin)

        case (_: HiveDecimalObjectInspector, DecimalType.Fixed(p, _)) =>
          append(
            s"""
               |
             """.stripMargin)

          appendNullSafeSimpleData(classOf[HiveDecimalObjectInspector])
          ???
      }
    }.mkString("\n")

    val code =
      s"""import ${classOf[Iterator[_]].getName};
         |
         |import ${classOf[BufferHolder].getName};
         |import ${classOf[Decimal].getName};
         |import ${classOf[UnsafeRowWriter].getName};
         |
         |import ${classOf[HiveField].getName};
         |import ${classOf[OrcSerde].getName};
         |import ${classOf[StructObjectInspector].getName};
         |
         |public SpecificUnsafeHiveIterator generate($exprType[] expr) {
         |  return new SpecificUnsafeHiveIterator();
         |}
         |
         |class SpecificUnsafeHiveIterator extends ${classOf[UnsafeHiveIterator].getName} {
         |  private OrcSerde deserializer;
         |  private Iterator writables;
         |  private StructObjectInspector structOI;
         |  private FieldWrapper[] fieldWrappers;
         |
         |  private BufferHolder bufferHolder;
         |  private UnsafeRowWriter unsafeRowWriter;
         |  private UnsafeRow unsafeRow;
         |
         |  public void initialize(
         |    Iterator writables,
         |    FieldWrapper[] fieldWrappers,
         |    StructObjectInspector structOI
         |  ) {
         |    this.deserializer = new OrcSerde;
         |    this.writables = writables;
         |    this.structOI = structOI;
         |    this.fieldWrappers = fieldWrappers;
         |
         |    fieldRefs = new StructField[fie];
         |
         |    this.bufferHolder = new BufferHolder();
         |    this.unsafeRowWriter = new UnsafeRowWriter();
         |    this.unsafeRow = new UnsafeRow();
         |  }
         |
         |  public boolean hasNext() {
         |    return writables.hasNext();
         |  }
         |
         |  public InternalRow next() {
         |    bufferHolder.reset();
         |    unsafeRowWriter.initialize(bufferHolder, 2);
         |
         |    Writable raw = deserializer.deserialize(writables.next());
         |    $writeToUnsafeRow
         |
         |    unsafeRowWriter.write(0, 1L);
         |    unsafeRowWriter.write(1, 1.5F);
         |    unsafeRow.pointTo(bufferHolder.buffer, 2, bufferHolder.totalSize());
         |    return unsafeRow;
         |  }
         |}
       """.stripMargin

    logDebug {
      val className = classOf[UnsafeHiveIterator].getSimpleName
      s"Generated $className: ${CodeFormatter.format(code)}"
    }

    compile(code).generate(context.references.toArray).asInstanceOf[UnsafeHiveIterator]
  }
}

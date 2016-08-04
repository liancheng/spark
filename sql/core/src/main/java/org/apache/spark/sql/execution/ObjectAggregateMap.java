package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;

public final class ObjectAggregateMap {
  private final Map<InternalRow, MutableRow> hashMap = new LinkedHashMap<>();

  private final MutableRow emptyAggregationBuffer;

  private final StructType aggregationBufferSchema;

  public ObjectAggregateMap(
      MutableRow emptyAggregationBuffer,
      StructType aggregationBufferSchema) {
    this.emptyAggregationBuffer = emptyAggregationBuffer;
    this.aggregationBufferSchema = aggregationBufferSchema;
  }

  public MutableRow getAggregationBufferByKey(MutableRow groupingKey) {
    MutableRow aggregationBuffer = hashMap.get(groupingKey);
    if (aggregationBuffer == null) {
      aggregationBuffer = new SpecificMutableRow(aggregationBufferSchema);
      aggregationBuffer.copyFrom(emptyAggregationBuffer, aggregationBufferSchema);
    }

    return aggregationBuffer;
  }

  public KVIterator<InternalRow, MutableRow> iterator() {
    return new KVIterator<InternalRow, MutableRow>() {
      private final Iterator<Map.Entry<InternalRow, MutableRow>> iterator =
          hashMap.entrySet().iterator();

      private Map.Entry<InternalRow, MutableRow> current = null;

      @Override
      public boolean next() throws IOException {
        boolean hasNext = iterator.hasNext();
        if (hasNext) {
          current = iterator.next();
        }

        return hasNext;
      }

      @Override
      public InternalRow getKey() {
        return current.getKey();
      }

      @Override
      public MutableRow getValue() {
        return current.getValue();
      }

      @Override
      public void close() {}
    };
  }

  public void free() {
    hashMap.clear();
  }
}

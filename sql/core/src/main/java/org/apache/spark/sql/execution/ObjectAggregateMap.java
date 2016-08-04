package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.unsafe.KVIterator;

public final class ObjectAggregateMap {
  public interface AggregationBufferInitializer {
    MutableRow initialize();
  }

  private final Map<InternalRow, MutableRow> hashMap = new LinkedHashMap<>();

  private final AggregationBufferInitializer aggregationBufferInitializer;

  public ObjectAggregateMap(AggregationBufferInitializer aggregationBufferInitializer) {
    this.aggregationBufferInitializer = aggregationBufferInitializer;
  }

  public MutableRow getAggregationBufferByKey(MutableRow groupingKey) {
    MutableRow aggregationBuffer = hashMap.get(groupingKey);
    if (aggregationBuffer == null) {
      aggregationBuffer = aggregationBufferInitializer.initialize();
      hashMap.put(groupingKey, aggregationBuffer);
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.io.IOException;


import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<RowData> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final boolean upsert;
  private final boolean upsertPart;
  private StreamExecutionEnvironment env = null;
  private Table table = null;

  BaseDeltaTaskWriter(PartitionSpec spec,
                      FileFormat format,
                      FileAppenderFactory<RowData> appenderFactory,
                      OutputFileFactory fileFactory,
                      FileIO io,
                      long targetFileSize,
                      Schema schema,
                      RowType flinkSchema,
                      List<Integer> equalityFieldIds,
                      boolean upsert,
                      boolean upsertPart) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.upsert = upsert;
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.upsertPart = upsertPart;
  }

  abstract RowDataDeltaWriter route(RowData row);

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row);
    Record raw = null;

    try {
      Types.NestedField id = deleteSchema.caseInsensitiveFindField("id");
      int fieldId = id.fieldId();
      if (upsertPart) {
        int ids = row.getInt(fieldId - 1);
        Expression expression = Expressions.equal("id", ids);
        if(table == null) {
          String loc = writer.getLocation();
          TableLoader loader = TableLoader.fromHadoopTable(loc);
          loader.open();
          table = loader.loadTable();
        }
        CloseableIterable<Record> iterable = IcebergGenerics
                .read(table)
                .where(expression)
                .build();
        //由于主键的设置，此处应该只有一条数据,直接赋值即可
        for (Record record : iterable) {
          raw = record;
        }
        if(raw != null) {
          GenericRowData newRow = new GenericRowData(row.getRowKind(), row.getArity());
          RowType rowType = FlinkSchemaUtil.convert(schema);
          for (int i = 0; i < row.getArity(); i++) {
            LogicalType type = rowType.getTypeAt(i);
            RowData.FieldGetter getter = RowData.createFieldGetter(type, i);
            Object rawVal = raw.get(i);
            Object rowVal = getter.getFieldOrNull(row);
            Object newValue = rowVal != null ? rowVal : rawVal;
            newRow.setField(i, newValue);
          }
          row = newRow;
        }
//        System.out.println("raw: "+ raw + " row" + row);
      }
    }catch (Exception e){
      System.out.println("******** get record error");
      e.printStackTrace();
    }
    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        if (upsert) {
          writer.delete(row);
        }
        writer.write(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break;  // UPDATE_BEFORE is not necessary for UPDATE, we do nothing to prevent delete one row twice
        }
        writer.delete(row);
        break;
      case DELETE:
        writer.delete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }



  protected class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
    RowDataDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
    }

    @Override
    protected StructLike asStructLike(RowData data) {
      return wrapper.wrap(data);
    }
  }
}

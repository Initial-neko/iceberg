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


import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
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
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<RowData> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final boolean upsert;
  private final boolean upsertPart;
  private StreamExecutionEnvironment env = null;
  private Table table = null;
  private int processRowNum = 50;
  private final List<RowData> rowList = Lists.newArrayList();

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
                      boolean upsertPart,
                      Table table) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.upsert = upsert;
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.upsertPart = upsertPart;
    this.table = table;
  }

  abstract RowDataDeltaWriter route(RowData row);

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public WriteResult complete() throws IOException {
    MyProcess();
    return super.complete();
  }

  public void MyProcess() throws IOException {
    System.out.println("now:" + rowList);
    ArrayList<RowData> rowListCopy = new ArrayList<RowData>();
    synchronized (rowList) {
      rowListCopy = Lists.newArrayList(rowList);
      rowList.clear();
    }
    if(rowListCopy.size() != 0) {
      //获取样板row，通过样板row获取row getter，如果会发生schema变更的情况，那么需要对row进行分组
      RowData templateRow = rowListCopy.get(0);
      Set<Integer> fieldIds = table.schema().identifierFieldIds();
      Expression finalExpression = Expressions.alwaysTrue();

      // cc @RowDataWrapper can build this more greater
      // RowDataWrapper wrap = wrapper().wrap(templateRow);

      RowType rowType = FlinkSchemaUtil.convert(schema);
      RowData.FieldGetter[] getters = new RowData.FieldGetter[templateRow.getArity()];
      for (int i = 0; i < templateRow.getArity(); i++) {
        LogicalType type = rowType.getTypeAt(i);
        getters[i] = RowData.createFieldGetter(type, i);
      }

      for (RowData row : rowListCopy) {
        Expression expression = Expressions.alwaysTrue();
        for (Integer fieldId : fieldIds) {
          Object fieldVal = getters[fieldId - 1].getFieldOrNull(row);
          String fieldName = schema.findColumnName(fieldId);
          if (fieldVal instanceof BinaryStringData) {
            fieldVal = fieldVal.toString();
          }
          expression = Expressions.and(expression, Expressions.equal(fieldName, fieldVal));
        }
        finalExpression = Expressions.or(finalExpression, expression);
      }

      CloseableIterable<Record> iterable = IcebergGenerics
              .read(table)
              .where(finalExpression)
              .build();

      //构建multiMap，通过identifier进行分组，便于后续的查询
      //实际上这个Record只会有一条
      Map<String, List<Record>> collect = Lists.newArrayList(iterable).stream().collect(Collectors.groupingBy((record) -> {
        StringBuilder group = new StringBuilder();
        for (Integer id : fieldIds) {
          group.append(record.get(id));
        }
        return group.toString();
      }));

      for (RowData row : rowListCopy) {
        Record raw = null;
        StringBuilder key = new StringBuilder();
        for(int id: fieldIds){
          key.append(getters[id].getFieldOrNull(row));
        }
        //组装对应的record
        raw = collect.get(key.toString()).get(0);
        if (raw != null) {
          GenericRowData newRow = new GenericRowData(row.getRowKind(), row.getArity());
          for (int i = 0; i < row.getArity(); i++) {
            Object rawVal = raw.get(i);
            Object rowVal = getters[i].getFieldOrNull(row);
            Object newValue = rowVal != null ? rowVal : rawVal;
            newRow.setField(i, newValue);
          }
          write(newRow, true);
        }
      }
    }
  }

  public void write(RowData row, boolean flag) throws IOException {
    RowDataDeltaWriter writer = route(row);

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

  @Override
  public void write(RowData row) throws IOException {
    if(upsertPart){
      rowList.add(row);
      //可以加入时间判断，比如每次1秒就直接执行MyProcess发出write事件
      if(rowList.size() >= processRowNum){
        MyProcess();
      }
      return;
    }
    RowDataDeltaWriter writer = route(row);
    Record raw = null;

    try {

      if (upsertPart) {
        Set<Integer> fieldIds = table.schema().identifierFieldIds();
        Expression expression = Expressions.alwaysTrue();

        // cc @RowDataWrapper can build this more greater
        RowType rowType = FlinkSchemaUtil.convert(schema);
        RowData.FieldGetter[] getters = new RowData.FieldGetter[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
          LogicalType type = rowType.getTypeAt(i);
          getters[i] = RowData.createFieldGetter(type, i);
        }

        for(Integer fieldId: fieldIds) {
          Object fieldVal = getters[fieldId - 1].getFieldOrNull(row);
          String fieldName = schema.findColumnName(fieldId);
          if(fieldVal instanceof BinaryStringData){
            fieldVal = fieldVal.toString();
          }
          expression = Expressions.and(expression, Expressions.equal(fieldName, fieldVal));
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
          for (int i = 0; i < row.getArity(); i++) {
            Object rawVal = raw.get(i);
            Object rowVal = getters[i].getFieldOrNull(row);
            Object newValue = rowVal != null ? rowVal : rawVal;
            newRow.setField(i, newValue);
          }
//          System.out.println("raw: "+ raw + " row " + row + " newrow " + newRow);
          row = newRow;
        }

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

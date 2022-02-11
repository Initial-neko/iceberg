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

import com.bj58.dsap.api.scf.contract.IHbaseApi;
import com.bj58.dsap.api.scf.contract.Result;
import com.bj58.dsap.api.scf.module.Put;
import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.spat.scf.protocol.serializer.SerializeBase;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.fasterxml.jackson.core.SerializableString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.input.CharSequenceInputStream;
import org.apache.flink.api.common.typeutils.TypeSerializerUtils;
import org.apache.flink.api.java.typeutils.runtime.KryoUtils;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.io.MD5Hash;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.SerializationUtil;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<RowData> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final boolean upsert;
  private final boolean upsertPart;
  private StreamExecutionEnvironment env = null;
  private Table table = null;

  //hbase相关服务
  public final static String url = "tcp://dsapapi/HbaseApi";
  public static final String tableName = "iceberg_index";
  public static final String columnFamily = "f1";
  static IHbaseApi service = null;
  public static RowData.FieldGetter[] getters;
  RowDataSerializer rowDataSerializer = null;

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
    System.setProperty("scf.serializer.basepakage", "com.bj58");
    SCFInit.initScfKeyByValue("qz9ZoZWTD2LyATBp4BHDM4bw2TtSqPLR");
    service = ProxyFactory.create(IHbaseApi.class, url);
    rowDataSerializer = new RowDataSerializer(flinkSchema);
  }

  abstract RowDataDeltaWriter route(RowData row);

  RowDataWrapper wrapper() {
    return wrapper;
  }

  public String serialize(RowData obj) throws IOException {

//    System.out.println("start:" + obj);
    long start = System.currentTimeMillis();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputView dataOutputView = new DataOutputViewStreamWrapper(outputStream);

    rowDataSerializer.serialize(obj , dataOutputView);
    String outStr = "";
//    dataOutputView.writeUTF(outStr);
    outStr = outputStream.toString("UTF-8");
//    System.out.println("out:" + outStr);
    long end = System.currentTimeMillis();
    System.out.println("serialize:" + (double)(end - start) / 1000);
    return outStr;
  }

  public RowData deserialize(String srcStr) throws IOException {

    long start = System.currentTimeMillis();
    DataInputView dataInputView = new DataInputViewStreamWrapper(new CharSequenceInputStream(srcStr, StandardCharsets.UTF_8));
    RowData rowData = rowDataSerializer.deserialize(dataInputView);
    if(rowData instanceof BinaryRowData){
      GenericRowData newRow = new GenericRowData(rowData.getRowKind(), rowData.getArity());
      for (int i = 0; i < rowData.getArity(); i++) {
        Object rowVal = getters[i].getFieldOrNull(rowData);
        newRow.setField(i, rowVal);
      }
      rowData = newRow;
    }
//    System.out.println("deserialize:" + rowData);
    long end = System.currentTimeMillis();
    System.out.println("deserialize:" + (double)(end - start) / 1000);
    return rowData;
  }

  //rowString +I(1,2,3)
  /*GenericRowData deSerialize(String rowString){

    GenericRowData row = new GenericRowData(RowKind.INSERT, getters.length);
    //1,2,3)
    String split = rowString.substring(3);
    //1,2,3
    String split2 = split.substring(0, split.length() - 1);
    String[] values = split2.split(",");
    for (int i = 0; i < row.getArity(); i++) {
      row.setField(i, values[i]);
    }
    return row;
  }*/

  GenericRowData getFromHbase(String id) throws IOException {
    long start = System.currentTimeMillis();

    Result result = service.queryByRowKey(tableName, String.valueOf(id));
    String value = (String)result.getScfMap().get(columnFamily + ":value");
    GenericRowData row = null;
    if(value != null){
//      row = deSerialize(value);
      row = (GenericRowData) deserialize(value);
    }
    long end = System.currentTimeMillis();
    System.out.println("getFromHbase(deserialize):" + (double)(end - start) / 1000);
    return row;
  }

  void writeToHbase(String id, RowData row) throws IOException {
    LinkedList<Put> rowInfo = new LinkedList<>();
    Put put = new Put();
    put.setRowKey(String.valueOf(id));
    put.setColumnName("value");

    /*GenericRowData newRow = null;
    if(row instanceof BinaryRowData){
      newRow = new GenericRowData(row.getRowKind(), row.getArity());
      for (int i = 0; i < row.getArity(); i++) {
        Object rowVal = getters[i].getFieldOrNull(row);
        newRow.setField(i, rowVal);
      }
      row = newRow;
    }*/
    put.setColumnValue(serialize(row));
//    put.setColumnValue(((GenericRowData)row).toString());
    rowInfo.add(put);
    try {
      long start = System.currentTimeMillis();
      service.insert(tableName, columnFamily, rowInfo);
      long end = System.currentTimeMillis();
      System.out.println("writeToHbase(insert):" + (double)(end - start) / 1000);
    } catch (Exception e) {
      System.out.println("hbase insert error");
      e.printStackTrace();
    }
  }

  @Override
  public void write(RowData row) throws IOException {
    long start = System.currentTimeMillis();
    RowDataDeltaWriter writer = route(row);
    GenericRowData raw = null;
    String id = "";
    try {

      if (upsertPart) {
        Set<Integer> fieldIds = table.schema().identifierFieldIds();

        // cc @RowDataWrapper can build this more greater
        RowType rowType = FlinkSchemaUtil.convert(schema);
        if(getters == null) {
          getters = new RowData.FieldGetter[row.getArity()];
          for (int i = 0; i < row.getArity(); i++) {
            LogicalType type = rowType.getTypeAt(i);
            getters[i] = RowData.createFieldGetter(type, i);
          }
        }

        //主键为1个字段，这个时候循环只会执行一次
        for(Integer fieldId: fieldIds) {
          Object fieldVal = getters[fieldId - 1].getFieldOrNull(row);
          String fieldName = schema.findColumnName(fieldId);
          if(fieldVal instanceof BinaryStringData){
            fieldVal = fieldVal.toString();
          }
          if(fieldVal instanceof Integer){
            id = String.valueOf(fieldVal);
          }else{
            id = (String)fieldVal;
          }
          raw = getFromHbase(id);
        }
        //采用hbase index来进行字段的更新

        if(raw != null) {
          GenericRowData newRow = new GenericRowData(row.getRowKind(), row.getArity());

          for (int i = 0; i < row.getArity(); i++) {
            Object rawVal = getters[i].getFieldOrNull(raw);
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
        if(upsertPart) {
          writeToHbase(id, row);
        }
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break;  // UPDATE_BEFORE is not necessary for UPDATE, we do nothing to prevent delete one row twice
        }
        writer.delete(row);
        break;
      case DELETE:
        writer.delete(row);
        if(upsertPart) {
          writeToHbase(id, null);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
    long end = System.currentTimeMillis();
    System.out.println("write:" + (double)(end - start) / 1000);
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

package org.apache.iceberg.flink;

import com.bj58.dsap.api.scf.contract.IHbaseApi;
import com.bj58.dsap.api.scf.contract.Result;
import com.bj58.dsap.api.scf.module.Put;
import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import org.apache.commons.io.input.CharSequenceInputStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Collector;

import org.apache.iceberg.hive.HiveCatalog;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;


public class Demo_SCF {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db1";
    HiveCatalog catalog = null;
    String warehouse = "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog";
    String TABLE_NAME = "iceberg_insert501";
    static int checkpoint = 10;
    static int ts_compare;
    public final static String url = "tcp://dsapapi/HbaseApi";
    public static final String tableName = "iceberg_index";
    public static final String columnFamily = "f1";
    static IHbaseApi service = null;
    public static RowData.FieldGetter[] getters;
    RowDataSerializer rowDataSerializer = null;

    @BeforeClass
    public static void setup(){
//        System.setProperty("HADOOP_USER_NAME", "hdp_lbg_supin");
    }

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("rest.port", "8081-8089");
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        System.setProperty("scf.serializer.basepakage", "com.bj58");
        SCFInit.initScfKeyByValue("qz9ZoZWTD2LyATBp4BHDM4bw2TtSqPLR");
        service = ProxyFactory.create(IHbaseApi.class, url);

    }

    static class MyProcessWindowFunction
            extends ProcessAllWindowFunction<Tuple3<Integer, Integer, Long>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple3<Integer, Integer, Long>> elements, Collector<String> out) throws Exception {

            elements.forEach((tuple) -> {
                LinkedList<Put> rowInfo = new LinkedList<>();
                Put put = new Put();
                put.setRowKey(String.valueOf(tuple.f0));
                put.setColumnName("value");

                put.setColumnValue(String.valueOf(tuple.f1));
                rowInfo.add(put);

                long start = System.currentTimeMillis();
                try {
                    service.insert(tableName, columnFamily, rowInfo);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
//                System.out.println("writeToHbase(insert):" + (double)(end - start) / 1000);
                count++;
                System.out.println("count:" + count);
            });
            out.collect("count:" + count);
        }
    }

    public static long count = 0;


    static class MyProcessWindowFunction2
            extends ProcessFunction<Tuple3<Integer, Integer, Long>, Integer> {


        @Override
        public void processElement(Tuple3<Integer, Integer, Long> tuple, Context ctx, Collector<Integer> out) throws Exception {

            LinkedList<Put> rowInfo = new LinkedList<>();
            Put put = new Put();
            put.setRowKey(String.valueOf(tuple.f0));
            put.setColumnName("value");

            put.setColumnValue(String.valueOf(tuple.f1));
            rowInfo.add(put);

            long start = System.currentTimeMillis();
            try {
                service.insert(tableName, columnFamily, rowInfo);
            } catch (Exception e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
//            System.out.println("writeToHbase(insert):" + (double)(end - start) / 1000);
            count++;
            System.out.println("count:" + count + "\r");
        }
    }


    static class MySourceFunction implements SourceFunction<Tuple3<Integer, Integer, Long>> {

        boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
            while(isRunning){
                Random random = new Random();
                for(int i = 0; i < 500; i++){
                    int id = (int)(random.nextDouble() * 20 + 1);
                    int data1 = (int)(random.nextGaussian() * 20 + 1);
                    long timeStamp = System.currentTimeMillis();
                    ctx.collect(new Tuple3<>(id, data1, timeStamp));
                }
                Thread.sleep( 6000);
//                isRunning = false;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    @Test
    public void TestDataStreamInsert() throws Exception {

//        TABLE_NAME = "test_upsert_11";
        DataStreamSource<Tuple3<Integer, Integer, Long>> dataStream = env.addSource(new MySourceFunction());


        /*dataStream.
                process(new MyProcessWindowFunction2());*/

        dataStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute("test");

        Thread.sleep(20000);
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

}




package org.apache.iceberg.flink;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import scala.collection.convert.Wrappers;

public class Demo2 {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db1";
    HiveCatalog catalog = null;
    String warehouse = "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog";
    String TABLE_NAME = "iceberg_insert501";
    int checkpoint = 600;
    static int ts_compare;

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
        env.setParallelism(1);
//flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(checkpoint * 1000);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create CATALOG iceberg_hadoop_catalog with" +
                "('type'='iceberg','catalog-type'='hive','uri'='thrift://10.162.12.69:9083'," +
                "'warehouse'='hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog')");


        tenv.useCatalog("iceberg_hadoop_catalog");
        tenv.useDatabase("hdp_teu_dpd_default_stream_db1");
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();
//        configuration.setString("execution.type", "batch");
        configuration.setString("table.dynamic-table-options.enabled", "true");
        catalog = getCatalog();
    }

    public HiveCatalog getCatalog() {
        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hive");
        config.put("uri", "thrift://10.162.12.69:9083");
        config.put("warehouse", warehouse);
        HiveCatalog hiveCatalog = new HiveCatalog(new Configuration());
        hiveCatalog.initialize("iceberg_hive_catalog", config);
        return hiveCatalog;
    }

    public static long count = 0;

    static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, RowData>, RowData, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, RowData>> input, Collector<RowData> out) {

            ArrayList<Tuple2<String, RowData>> tuple2s = Lists.newArrayList(input);
            tuple2s.stream()
                    .map((tuple) -> tuple.f1)
                    .sorted((a, b) -> (int) (a.getLong(ts_compare) - b.getLong(ts_compare)))
                    .collect(Collectors.toList()).forEach(out::collect);

//            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
    static class MySourceFunction implements SourceFunction<Tuple3<Integer, Integer, Long>> {

        boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
            while(isRunning){
                Random random = new Random();
                for(int i = 0; i < 10000; i++){
                    int id = (int)(random.nextDouble() * 1000000 + 1);
                    int data1 = (int)(random.nextGaussian() * 1000000 + 1);
                    long timeStamp = System.currentTimeMillis();
                    ctx.collect(new Tuple3<>(id, data1, timeStamp));
                }
//                Thread.sleep(6000);
                isRunning = false;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    @Test
    public void TestDataStreamInsert() throws InterruptedException {

        TABLE_NAME = "test_upsert_11";
        DataStreamSource<Tuple3<Integer, Integer, Long>> dataStream = env.addSource(new MySourceFunction());

        org.apache.flink.table.api.Table table = tenv.fromDataStream(dataStream,
                Schema.newBuilder()
                        .column("f0", "INT")
                        .column("f1", "INT")
                        .column("f2", "BIGINT")
                        .build());
        tenv.createTemporaryView("table", table);
        catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        long start = System.currentTimeMillis();
        tenv.executeSql(String.format("insert into %s(id, data1, ts) SELECT f0 as id, f1 as data1, f2 as ts from `table`", TABLE_NAME)).print();
        long end = System.currentTimeMillis();
        System.out.println("=========================== " + (double)(end - start) / 1000);
        Thread.sleep(20000);
    }

    @Test
    public void TestDataStreamInsertTime() throws InterruptedException {

        TABLE_NAME = "test_upsert_15";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  `ts`   BIGINT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");
        DataStreamSource<Tuple3<Integer, Integer, Long>> dataStream = env.addSource(new MySourceFunction());

        org.apache.flink.table.api.Table table = tenv.fromDataStream(dataStream,
                Schema.newBuilder()
                        .column("f0", "INT")
                        .column("f1", "INT")
                        .column("f2", "BIGINT")
                        .build());
        tenv.createTemporaryView("table", table);
        catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        long start = System.currentTimeMillis();
        tenv.executeSql(String.format("insert into %s(id, data1, ts) SELECT f0 as id, f1 as data1, f2 as ts from `table`", TABLE_NAME)).print();
        long end = System.currentTimeMillis();
        System.out.println("=========================== " + (double)(end - start) / 1000);
        Thread.sleep(20000);
    }

    @Test
    public void test_print() throws Exception {
        TABLE_NAME = "test_upsert_10";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  `ts`   LONG," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true')");
        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
//        tenv.executeSql("ALTER table "+TABLE_NAME+" set('write.upsert-part.enable'='true')");
        // tenv.executeSql("select * from iceberg_insert500").print();
        Configuration conf = catalog.getConf();
        String uri = conf.get("hive.metastore.uris");
        String warehouse = conf.get("hive.metastore.warehouse.dir");
        HashMap<String, String> config = new HashMap<>();
        config.put("uri", uri);
        config.put("warehouse", warehouse);

        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("catalog", catalog.getConf(), config), TableIdentifier.of(database, TABLE_NAME));
        tableLoader.open();

       /* for (Snapshot snapshot : table.snapshots()) {
            System.out.println(snapshot);
        }*/
        List<Long> snapshots = SnapshotUtil.currentAncestors(table);

        Set<Integer> fieldIds = table.schema().identifierFieldIds();
        org.apache.iceberg.Schema schema = table.schema();
        RowType rowType = FlinkSchemaUtil.convert(schema);
//        RowDataWrapper wrapper = new RowDataWrapper(rowType, schema.asStruct());
        RowData.FieldGetter[] getters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType type = rowType.getTypeAt(i);
            getters[i] = RowData.createFieldGetter(type, i);
        }
        Types.NestedField ts = schema.findField("ts");
        ts_compare = ts.fieldId();

        FlinkSource.forRowData()
                .env(env)
                .table(table)
                .tableLoader(tableLoader)
                .streaming(true)
                .startSnapshotId(snapshots.get(0))
                .build()
                .map(new MapFunction<RowData, Tuple2<String, RowData>>() {
                    @Override
                    public Tuple2<String, RowData> map(RowData rowData) throws Exception {
                        StringBuilder group = new StringBuilder();
                        for (Integer id : fieldIds) {
                            //通过fieldIDS进行分组即可
                            group.append(getters[id].getFieldOrNull(rowData) + "_");
                        }
                        System.out.println(group + " " + rowData);
                        return Tuple2.of(group.toString(), rowData);
                    }
                })
                .setParallelism(1)
                //按照刚才指定的key进行分流
                .keyBy(f -> f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(checkpoint)))
                .process(new MyProcessWindowFunction())
                .print();

//        rowDataStream.print();
//        tenv.executeSql("select count(*) from "+TABLE_NAME+"").print();
        env.execute("test");
    }

    @Test
    public void test_print2() throws Exception {
        TABLE_NAME = "test_upsert_11";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  `ts`   BIGINT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true')");
        tenv.executeSql("alter table "+TABLE_NAME+" set('write.upsert-part.enable'='true')");
        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
//        tenv.executeSql("ALTER table "+TABLE_NAME+" set('write.upsert-part.enable'='true')");
        // tenv.executeSql("select * from iceberg_insert500").print();
        Configuration conf = catalog.getConf();
        String uri = conf.get("hive.metastore.uris");
        String warehouse = conf.get("hive.metastore.warehouse.dir");
        HashMap<String, String> config = new HashMap<>();
        config.put("uri", uri);
        config.put("warehouse", warehouse);

        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("catalog", catalog.getConf(), config), TableIdentifier.of(database, TABLE_NAME));
        tableLoader.open();

       /* for (Snapshot snapshot : table.snapshots()) {
            System.out.println(snapshot);
        }*/
        List<Long> snapshots = SnapshotUtil.currentAncestors(table);

        Set<Integer> fieldIds = table.schema().identifierFieldIds();
        org.apache.iceberg.Schema schema = table.schema();
        RowType rowType = FlinkSchemaUtil.convert(schema);
//        RowDataWrapper wrapper = new RowDataWrapper(rowType, schema.asStruct());
        RowData.FieldGetter[] getters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            LogicalType type = rowType.getTypeAt(i);
            getters[i] = RowData.createFieldGetter(type, i);
        }
        Types.NestedField ts = schema.findField("ts");
        ts_compare = ts.fieldId();

        DataStream<RowData> dataStream = FlinkSource.forRowData()
                .env(env)
                .table(table)
                .tableLoader(tableLoader)
                .streaming(true)
                .startSnapshotId(snapshots.get(0))
                .build();
//                .print();

//        rowDataStream.print();
//        tenv.executeSql("select count(*) from "+TABLE_NAME+"").print();
        env.execute("test");
    }


}




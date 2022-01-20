/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStreamConsume {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db1";
    HiveCatalog catalog = null;
    String TABLE_NAME = "test_db";

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("rest.port", "8081-8089");
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
//flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(10000);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create CATALOG iceberg_hadoop_catalog with" +
                "('type'='iceberg','catalog-type'='hive','uri'='thrift://10.162.12.69:9083'," +
                "'warehouse'='hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog')");

        /*tenv.executeSql("CREATE TABLE sourceTable (\n" +
                "user_id int,\n" +
                "f_random_str int\n" +
                ") WITH \n" +
                "('connector' = 'datagen',\n" +
                "'rows-per-second'='1000',\n" +
                "'fields.user_id.kind'='random',\n" +
                "'fields.user_id.max'='100',\n" +
                "'fields.user_id.min'='1',\n" +
                "'fields.f_random_str.kind'='random',\n" +
                "'fields.f_random_str.max'='100',\n" +
                "'fields.f_random_str.min'='1'\n" +
                ")");*/

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
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog");
        HiveCatalog hiveCatalog = new HiveCatalog(new Configuration());
        hiveCatalog.initialize("iceberg_hadoop_catalog", config);
        return hiveCatalog;
    }

    static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, RowData>, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, RowData>> input, Collector<String> out) {
            long count = 0;
            for (Tuple2<String, RowData> in: input) {
                count++;
            }
            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
    static class MySourceFunction implements SourceFunction<Tuple2<Integer, Integer>>{

        boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while(isRunning){
                Random random = new Random();
                for(int i = 0; i < 100; i++){
                    int id = (int)(random.nextDouble() * 100 + 1);
                    int data1 = (int)(random.nextGaussian() * 100 + 1);
                    ctx.collect(new Tuple2<>(id, data1));
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    @Test
    public void TestDataStreamInsert() throws InterruptedException {

        TABLE_NAME = "test_upsert_3";
        DataStreamSource<Tuple2<Integer, Integer>> dataStream = env.addSource(new MySourceFunction());

        org.apache.flink.table.api.Table table = tenv.fromDataStream(dataStream,
                Schema.newBuilder()
                        .column("f0", "INT")
                        .column("f1", "INT")
                        .build());
        tenv.createTemporaryView("table", table);
        catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        tenv.executeSql(String.format("insert into %s(id, data1) SELECT f0 as id, f1 as data1 from `table`", TABLE_NAME)).print();
        Thread.sleep(20000);
    }

    @Test
    public void test_print() throws Exception {
        TABLE_NAME = "test_upsert_3";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true')");
        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
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

        FlinkSource.forRowData()
                .env(env)
                .table(table)
                .tableLoader(tableLoader)
                .streaming(true)
                .build()
                .map(new MapFunction<RowData, Tuple2<String, RowData>>() {
                    @Override
                    public Tuple2<String, RowData> map(RowData rowData) throws Exception {
                        String key = String.valueOf(rowData.getInt(0));
                        return Tuple2.of(key, rowData);
                    }
                })
                .setParallelism(1)
                .keyBy(f -> f.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .process(new MyProcessWindowFunction())
                .print();

//        rowDataStream.print();
//        tenv.executeSql("select count(*) from "+TABLE_NAME+"").print();
        env.execute("test");
    }


    @Test
    public void test_non_upsert() {
        TABLE_NAME = "test_non_upsert3";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2')");
        tenv.executeSql("insert into " + TABLE_NAME + "(id,data1) " +
                "select user_id as id,f_random_str as data1 from default_catalog.default_database.sourceTable").print();
    }

    @Test
    public void test_upsert() {
        TABLE_NAME = "test_upsert_2";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true')");
        tenv.executeSql("insert into "+TABLE_NAME+"(id,data1) " +
                "select user_id as id,f_random_str as data1 from default_catalog.default_database.sourceTable").print();
    }

    @Test
    public void test_part_upsert() {
        TABLE_NAME = "test_db3368";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");
        tenv.executeSql("insert into "+TABLE_NAME+"(id,data1) " +
                "select user_id as id,f_random_str as data1 from default_catalog.default_database.sourceTable").print();
    }

}

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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Before;
import org.junit.Test;

public class TestStreamConsumeV1 {
    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db1";
    HiveCatalog catalog = null;

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

    /**
     * CREATE TABLE IF NOT EXISTS iceberg_insert( `id`  INT NOT NULL, `data1`   INT, `data2`   INT, PRIMARY KEY(id) NOT
     * ENFORCED ) with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='
     * true', 'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50');
     */
    @Test
    public void test() {
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert2(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50','write.upsert-part.enable'='true')");
        tenv.executeSql("insert into iceberg_insert2 values(1,2,3),(2,3,4),(3,4,5)");
        tenv.executeSql("insert into iceberg_insert2(id, data1) values(1,3)");
        tenv.executeSql("select * from iceberg_insert2 ").print();
    }

    /**
     * batch 模式不支持overwrite模式，所以如果采用主键进行insert会直接报错
     */
    @Test
    public void TestWithPrimaryBatch(){
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert2(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50','write.upsert-part.enable'='true')");

        //无需进行测试，目前尚不支持，可以通过流式的增量来修改batch方面的代码

        tenv.executeSql("drop table iceberg_insert2");
    }

    @Test
    public void TestWithNonPrimarBatch() throws InterruptedException {
        /*tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert2(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50','write.upsert-part.enable'='true')");*/
        String name = "iceberg_insert3";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+name+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2')");

        //s1 checkpoint设置为3秒 等待3秒顺序执行
        tenv.executeSql("insert into "+name+" values(1,2,3),(2,3,4),(3,4,5)");
        //Thread.sleep(4000);
        //s2
        tenv.executeSql("insert into "+name+"(id, data1) values(1,3),(2,4),(3,5)");
        //Thread.sleep(4000);
        //s3
        tenv.executeSql("insert into "+name+" values(4,5,6),(5,6,7),(6,7,8)");
        //Thread.sleep(4000);
        //s4
        tenv.executeSql("insert into "+name+"(id, data1) values(4,60),(5,70),(6,80)");

        //Thread.sleep(4000);
        Table table = catalog.loadTable(TableIdentifier.of(database, name));
        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        Map map = new HashMap<String, String>();
        //由于提交的顺序可能不一样，摄取最后2个snapshot的结果可能也会不同，但是现在已经设置了顺序提交了，所以每次结果都是相同的
        map.put("start-snapshot-id", Long.toString(ancestors.get(3)));
        map.put("end-snapshot-id", Long.toString(table.currentSnapshot().snapshotId()));
        map.put("streaming","false");
        tenv.executeSql("select * from "+name+" " + option(map)).print();

    }

    @Test
    public void TestWithPrimaryStream() throws IOException, InterruptedException {

        String name = "iceberg_insert5";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+name+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='5','write.upsert-part.enable'='true')");

        //采用流，可以用另外的测试来进行持续的插入获取结果
        //s1 checkpoint设置为3秒 等待3秒顺序执行
        tenv.executeSql("insert into "+name+" values(1,2,3),(2,3,4),(3,4,5)");
        Thread.sleep(3000);
        //s2
        tenv.executeSql("insert into "+name+" values(1,3,5),(2,4,6),(3,5,7)");
        Thread.sleep(3000);
        //s3
        tenv.executeSql("insert into "+name+" values(4,5,6),(5,6,7),(6,7,8)");
        Thread.sleep(3000);
        //s4
        tenv.executeSql("insert into "+name+"(id, data1) values(4,100),(5,200),(6,300)");

        Thread.sleep(3000);

        Thread.sleep(3000);
        Table table = catalog.loadTable(TableIdentifier.of(database, name));
        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        Map map = new HashMap<String, String>();
        //流只能采用当前快照，或者指定开始快照，会自动消费增量内容
        map.put("start-snapshot-id", Long.toString(ancestors.get(2)));
//        map.put("end-snapshot-id", Long.toString(table.currentSnapshot().snapshotId()));
        map.put("streaming","true");
        tenv.executeSql("select * from "+name+" " + option(map)).print();

    }

    @Test
    public void TestWithNonPrimaryStream() throws IOException, InterruptedException {

        String name = "iceberg_insert4";
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+name+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2')");


        //采用流，可以用另外的测试来进行持续的插入获取结果
        //s1 checkpoint设置为3秒 等待3秒顺序执行
        tenv.executeSql("insert into "+name+" values(1,2,3),(2,3,4),(3,4,5)");
        Thread.sleep(3000);
        //s2
        tenv.executeSql("insert into "+name+" values(1,3,5),(2,4,6),(3,5,7)");
        Thread.sleep(3000);
        //s3
        tenv.executeSql("insert into "+name+" values(4,5,6),(5,6,7),(6,7,8)");
        Thread.sleep(3000);
        //s4
        tenv.executeSql("insert into "+name+"(id, data1) values(4,100),(5,200),(6,300)");
        Thread.sleep(3000);
        Table table = catalog.loadTable(TableIdentifier.of(database, name));
        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        Map map = new HashMap<String, String>();
        //流只能采用当前快照，或者指定开始快照，会自动消费增量内容
        map.put("start-snapshot-id", Long.toString(ancestors.get(3)));
//        map.put("end-snapshot-id", Long.toString(table.currentSnapshot().snapshotId()));
        map.put("streaming","true");
        tenv.executeSql("select * from "+name+" " + option(map)).print();

    }



    protected String option(Map<String, String> sqlOptions) {

        StringBuilder builder = new StringBuilder();
        sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

        String optionStr = builder.toString();

        if (optionStr.endsWith(",")) {
            optionStr = optionStr.substring(0, optionStr.length() - 1);
        }

        if (!optionStr.isEmpty()) {
            optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
        }

        return optionStr;
    }

    private String optionToKv(String key, Object value) {
        return "'" + key + "'='" + value + "'";
    }

    //"select * from iceberg_insert /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/"
    @Test
    public void Hive_test() throws InterruptedException {

        /*tenv.executeSql("select * from iceberg_insert2").print();
        tenv.executeSql("insert into iceberg_insert2 values(1,2,3),(2,3,4),(3,4,5)");*/

        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hive");
        config.put("uri", "thrift://10.162.12.69:9083");
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog");
        BaseTable table = (BaseTable)catalog.loadTable(TableIdentifier.of(database, "iceberg_insert2"));
        Configuration conf = catalog.getConf();
        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("catalog", catalog.getConf(), config), TableIdentifier.of(database, "iceberg_insert2"));
        CatalogLoader catalogLoader = CatalogLoader.hive("catalog", this.catalog.getConf(), config);
        tableLoader.open();
        Table table1 = tableLoader.loadTable();
        List<Long> snapshots = SnapshotUtil.currentAncestors(table);
        System.out.println(snapshots);
        /*TableSchema projectedSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                //.field("data1", DataTypes.STRING())
                .field("data2", DataTypes.STRING()).build();*/

//        System.out.println(table.currentSnapshot().snapshotId()+" ~~ " +snapshots);
        /*System.out.println(table1);
        FlinkSource.forRowData()
                .env(env)
                .table(table)
                .tableLoader(tableLoader)
                .streaming(true)
                //.startSnapshotId(snapshots.get(snapshots.size() - 1))
                //.endSnapshotId(snapshots.get(0))
                //.project(projectedSchema)
                .build().print();*/
        tenv.executeSql("insert into iceberg_insert2(id,data1) values(1,200),(2,300),(3,400)");
        tenv.executeSql("select * from iceberg_insert2 /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/").print();
    }

    @Test
    public void Hive_test2() throws Exception {
//        tenv.executeSql("alter table iceberg_insert2 set('write.upsert-part.enable'='true')");
//        tenv.executeSql("select * from iceberg_insert2").print();
//        tenv.executeSql("insert into iceberg_insert2 values(1,2,3),(2,3,4),(3,4,5)");

        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_insert2"));
        Configuration conf = catalog.getConf();
        String uri = conf.get("hive.metastore.uris");
        String warehouse = conf.get("hive.metastore.warehouse.dir");
        HashMap<String, String> config = new HashMap<>();
        config.put("uri", uri);
        config.put("warehouse", warehouse);
        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("catalog", catalog.getConf(), config), TableIdentifier.of(database, "iceberg_insert2"));
        CatalogLoader catalogLoader = CatalogLoader.hive("catalog", this.catalog.getConf(), config);
        tableLoader.open();
        Table table1 = tableLoader.loadTable();
        List<Long> snapshots = SnapshotUtil.currentAncestors(table);
        System.out.println(snapshots);

        System.out.println(table1);
        FlinkSource.forRowData()
                .env(env)
                .table(table1)
                .tableLoader(tableLoader)
                .streaming(true)
                //.startSnapshotId(snapshots.get(snapshots.size() - 1))
                //.endSnapshotId(snapshots.get(0))
                //.project(projectedSchema)
                .build().print();


        env.execute("neko");
    }


    @Test
    public void Hive_test3() throws Exception {
        int random = (int)(Math.random() * 1000);
        String name = "iceberg_testInsert" + random;
        tenv.executeSql("drop table if exists "+name+"");
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+name+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='5','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, name));

        tenv.executeSql("insert into "+name+" values(1,2,3),(2,3,4),(3,4,5)");
        Thread.sleep(4000);

        tenv.executeSql("insert into "+name+"(id, data1) values(1,3),(2,4),(3,5)");
        Thread.sleep(4000);

        tenv.executeSql("insert into "+name+" values(4,5,6),(5,6,7),(6,7,8)");
        Thread.sleep(4000);

        tenv.executeSql("insert into "+name+"(id, data1) values(4,100),(5,200),(6,300)");

        Thread.sleep(5000);
        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        table.snapshots().forEach(System.out::println);
        long snapshotId1 = ancestors.get(ancestors.size() - 1);
        long snapshotId3 = ancestors.get(ancestors.size() - 3);
        long snapshotId4 = ancestors.get(ancestors.size() - 4);
//
//        tenv.executeSql(
//                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'start-snapshot-id'='%s', 'monitor-interval'='5s')*/",
//                        name, Long.toString(snapshotId1)));
        tenv.sqlQuery(String.format("select * from %s /*+ OPTIONS('streaming'='true', 'start-snapshot-id'='%s', 'monitor-interval'='5s')*/",
                name, Long.toString(snapshotId1)));
//        tenv.toChangelogStream();
//        tenv.executeSql(
//                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/",
//                        name)).print();
    }

    @Test
    public void StreamingInsertTest() throws Exception {

        String name = "iceberg_testInsert";

        tenv.executeSql("insert into "+name+" values(1,2,300),(2,3,400),(3,4,566)");

        tenv.executeSql("insert into "+name+" values(1,3,578),(2,4,678),(3,5,788)");

        tenv.executeSql("insert into "+name+" values(4,5,666),(5,6,756),(6,7,856)");

        tenv.executeSql("insert into "+name+"(id, data1) values(4,1700),(5,2700),(6,3070)");

    }
}



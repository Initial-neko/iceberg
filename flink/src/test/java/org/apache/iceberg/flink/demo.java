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

import java.util.HashMap;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class demo {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_lbg_supin_lakehouse";
    HiveCatalog catalog = null;
    String warehouse = "hdfs://10.162.12.100:9000/home/lakehouse/iceberg";
    String TABLE_NAME = "test_db";

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        System.out.println(System.getProperty("HADOOP_USER_NAME") + " " + System.getenv("HADOOP_USER_NAME"));
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("rest.port", "8081-8089");
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        //flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(5000);

        tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create CATALOG iceberg_hive_catalog with" +
                "('type'='iceberg','catalog-type'='hive','uri'='thrift://10.162.12.69:9083'," +
                "'warehouse'='" + warehouse + "')");
        tenv.useCatalog("iceberg_hive_catalog");
        tenv.useDatabase(database);
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();


//        configuration.setString("execution.type", "batch");
        //开启sql hint模式
        configuration.setString("table.dynamic-table-options.enabled", "true");
        catalog = getCatalog();
    }

    @After
    public void clean(){
        tenv.executeSql("drop table if exists "+TABLE_NAME+"");
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

    @Test
    public void test2(){
        System.out.println(123213);
    }

    /**
     * 测试建表以及插入参数， format-version选择2，upsert模式需要打开，部分字段模式开启，为三个测试中的主要参数（建表时需要保证有主键）
     * 建表的时候，可以设置任意主键和分区字段
     */
    @Test
    public void test_createAndInsert() {
//        System.setProperty("HADOOP_USER_NAME", "hdp_lbg_supin");
//        tenv.executeSql("select * from iceberg_insert007 ").print();
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert007(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");
        tenv.executeSql("insert into iceberg_insert007 values(1,2,3),(2,3,4),(3,4,5)");
        tenv.executeSql("select * from iceberg_insert007 ").print();
    }

    /**
     * SQL方式获取dataStream
     * @throws Exception
     */
    @Test
    public void TestStreamingReadingSQL() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));

        tenv.executeSql(String.format("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME));
        Thread.sleep(3000);
        //实际场景下，一定不要提交过多的insert，任务数目要少，因为iceberg的commit是单线程，以文件形式存储metadata，非常容易冲突，所以进行sleep减少冲突概率
        //可以的话，用流式insert来插入数据，本地测试的时候checkpoint可以开启的时间短一些（10秒左右），这样就会10秒一次提交，几乎不会冲突

        tenv.executeSql(String.format("insert into %s(id, data1) values(1,8),(2,9),(3,10)", TABLE_NAME));
        Thread.sleep(3000);

        tenv.executeSql(String.format("insert into %s(id, data2) values(1,30),(2,40),(3,50)", TABLE_NAME));
        Thread.sleep(3000);

        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        StreamExecutionEnvironment env = rowDataStream.getExecutionEnvironment();
        rowDataStream.print();
        env.execute("test with sql datastream");

    }

    @Test
    public void TestStreamingReadingAPI() throws Exception {

        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));

        tenv.executeSql(String.format("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME));
        Thread.sleep(3000);

        tenv.executeSql(String.format("insert into %s(id, data1) values(1,8),(2,9),(3,10)", TABLE_NAME));
        Thread.sleep(3000);

        tenv.executeSql(String.format("insert into %s(id, data2) values(1,30),(2,40),(3,50)", TABLE_NAME));
        Thread.sleep(3000);

        Configuration conf = catalog.getConf();
        String uri = conf.get("hive.metastore.uris");
        String warehouse = conf.get("hive.metastore.warehouse.dir");
        HashMap<String, String> config = new HashMap<>();
        config.put("uri", uri);
        config.put("warehouse", warehouse);

        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("catalog", catalog.getConf(), config), TableIdentifier.of(database, TABLE_NAME));

        tableLoader.open();

        List<Long> snapshots = SnapshotUtil.currentAncestors(table);
        System.out.println(snapshots);

        FlinkSource.forRowData()
                .env(env)
                .table(table)
                .tableLoader(tableLoader)
                .streaming(true)
                //"monitor-interval","10s"; 需要传入一个map，其实也可以把所有其他的参数都传到里面，效果是一样的
                .properties(new HashMap<>())
                //.startSnapshotId(snapshots.get(snapshots.size() - 1))
                .build().print();

        env.execute("API mode");

    }

    @Test
    public void TestContinuousSameKey() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));

        //没有得到想要的结果，就需要拉长等待时间，如果还没有提交就不会得到正确的结果，实际场景不会进行频繁提交，但这里需要测试效果，sleep时间需要调整
        tenv.executeSql(String.format("insert into %s values(1,2,3),(1,3,4),(1,4,5),(1,5,7),(1,100,200)", TABLE_NAME));
        Thread.sleep(5000);
        tenv.executeSql(String.format("insert into %s(id, data1) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);
        tenv.executeSql(String.format("insert into %s(id, data2) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);

        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        rowDataStream.print();
        env.execute("test with sql datastream");
    }

    @Test
    public void TestHiddenPartition() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        //iceberg可以通过时间来创建隐藏分区，但目前flink不支持直接建表就创建这样的分区
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ")with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        // 通过api进行修改，设置隐藏分区bucket ,day ,hour等
        table.updateSpec().addField(Expressions.bucket("id", 8)).commit();

        //没有得到想要的结果，就需要拉长等待时间，如果还没有提交就不会得到正确的结果，实际场景不会进行频繁提交，但这里需要测试效果，sleep时间需要调整
        tenv.executeSql(String.format("insert into %s values(1,2,3),(1,3,4),(1,4,5),(1,5,7),(1,100,200)", TABLE_NAME));
        Thread.sleep(5000);
        tenv.executeSql(String.format("insert into %s(id, data1) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);
        tenv.executeSql(String.format("insert into %s(id, data2) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);


        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        rowDataStream.print();
        env.execute("test with sql datastream");
    }

    /**
     * 普通字段partition
     * @throws Exception
     */
    @Test
    public void TestPartition2() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        //iceberg可以通过时间来创建隐藏分区，但目前flink不支持直接建表就创建这样的分区
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  `data3`   STRING NOT NULL," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ")PARTITIONED BY (data3) with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        // 通过api进行修改，设置隐藏分区bucket ,day ,hour等
//        table.updateSpec().addField(Expressions.bucket("id", 8)).commit();
        //需要将 分区字段加入identifierfields
        table.updateSchema().setIdentifierFields("id","data3").commit();


        //没有得到想要的结果，就需要拉长等待时间，如果还没有提交就不会得到正确的结果，实际场景不会进行频繁提交，但这里需要测试效果，sleep时间需要调整
        tenv.executeSql(String.format("insert into %s values(1,2,3,'part1'),(1,3,4,'part1'),(1,4,5,'part1'),(1,5,7,'part1'),(1,100,200,'part1')", TABLE_NAME));
        Thread.sleep(5000);

        //注意，分区字段和主键一样不可缺失，以下语句将报错
//        tenv.executeSql(String.format("insert into %s(id, data1) values(1,3),(1,5),(1,7)", TABLE_NAME));
        //注意，分区字段和主键一样不可缺失，以下语句将报错
//        tenv.executeSql(String.format("insert into %s(id, data2) values(1,3),(1,5),(1,7)", TABLE_NAME));

        //注意 分区字段也属于upsert主键的一部分，upsert主键必须包括（主键 + 分区字段），不然会出现重复记录，那么下列的插入将会进入分区part2
//        tenv.executeSql(String.format("insert into %s values(1,2,3,'part2'),(1,3,4,'part2'),(1,4,5,'part2'),(1,5,7,'part2'),(1,100,200,'part2')", TABLE_NAME));
        tenv.executeSql(String.format("insert into %s(id, data1, data3) values(1,3,'part2'),(1,4,'part2'),(1,5,'part2'),(1,7,'part2'),(1,200,'part2')", TABLE_NAME));


        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        rowDataStream.print();
        env.execute("test with sql datastream");
    }

    /**
     * 普通字段partition
     * @throws Exception
     */
    @Test
    public void TestPartition3() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "test_db" + (int)(Math.random() * 1000);

        //iceberg可以通过时间来创建隐藏分区，但目前flink不支持直接建表就创建这样的分区
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `data1`   INT," +
                "  `id`  INT NOT NULL," +
                "  `data2`   INT," +
                "  `data3`   STRING NOT NULL," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ")PARTITIONED BY (data3) with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");

        Table table = catalog.loadTable(TableIdentifier.of(database, TABLE_NAME));
        // 通过api进行修改，设置隐藏分区bucket ,day ,hour等
//        table.updateSpec().addField(Expressions.bucket("id", 8)).commit();
        //需要将 分区字段加入identifierfields
        table.updateSchema().setIdentifierFields("id", "data3").commit();


        //没有得到想要的结果，就需要拉长等待时间，如果还没有提交就不会得到正确的结果，实际场景不会进行频繁提交，但这里需要测试效果，sleep时间需要调整
        tenv.executeSql(String.format("insert into %s values(1,200,3,'part1'),(1,200,4,'part1'),(1,200,5,'part1'),(1,201,7,'part1'),(1,100,200,'part1')", TABLE_NAME));
        Thread.sleep(5000);

        //注意，分区字段和主键一样不可缺失，以下语句将报错
//        tenv.executeSql(String.format("insert into %s(id, data1) values(1,3),(1,5),(1,7)", TABLE_NAME));
        //注意，分区字段和主键一样不可缺失，以下语句将报错
//        tenv.executeSql(String.format("insert into %s(id, data2) values(1,3),(1,5),(1,7)", TABLE_NAME));

        //注意 分区字段也属于upsert主键的一部分，upsert主键必须包括（主键 + 分区字段），不然会出现重复记录，那么下列的插入将会进入分区part2
//        tenv.executeSql(String.format("insert into %s values(1,2,3,'part2'),(1,3,4,'part2'),(1,4,5,'part2'),(1,5,7,'part2'),(1,100,200,'part2')", TABLE_NAME));
//        tenv.executeSql(String.format("insert into %s(id, data1, data3) values(200,3,'part2'),(200,4,'part2'),(201,5,'part2'),(202,7,'part2'),(201,200,'part2')", TABLE_NAME));


        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        rowDataStream.print();
        env.execute("test with sql datastream");
    }

    @Test
    public void TestMonitor() throws Exception {

        //目前删除表可能不会清除所有目录，所以每次测试最好重新建表，结果更加清晰
        TABLE_NAME = "iceberg_monitor2";

        //iceberg可以通过时间来创建隐藏分区，但目前flink不支持直接建表就创建这样的分区
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')");


        for(int i = 0; i <= 100; i++){
            tenv.executeSql("insert into iceberg_monitor2 values(1,2,3),(1,3,4),(1,4,5),(1,5,6)");
            Thread.sleep(5000);
        }
    }
}



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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.In;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.flink.SimpleDataUtil.RECORD;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class myFlinkTest {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db";

    public HadoopCatalog getCatalog() {
        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hadoop");
        config.put("property-version", "1");
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog");
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration());
        hadoopCatalog.initialize("iceberg_hadoop_catalog", config);
        return hadoopCatalog;
    }
    @Before
    public void init(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("rest.port", "8081-8089");
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
//flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(60000);
        tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create CATALOG iceberg_hadoop_catalog with" +
                "('type'='iceberg','catalog-type'='hadoop'," +
                "'warehouse'='hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog')");
        tenv.useCatalog("iceberg_hadoop_catalog");
        tenv.useDatabase("hdp_teu_dpd_default_stream_db");
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();
        configuration.setString("execution.type", "batch");
        configuration.setString("table.dynamic-table-options.enabled", "true");
    }

    /**
     * CREATE TABLE IF NOT EXISTS iceberg_insert(
     *      `id`  INT NOT NULL,
     *      `data1`   INT,
     *      `data2`   INT,
     *      PRIMARY KEY(id) NOT ENFORCED
     *    ) with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='    true',
     *    'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50');
     */
    @Test
    public void test(){
        /*tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert2(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50')");*/
        tenv.executeSql("insert into iceberg_insert2 values(1,2,3),(2,3,4),(3,4,5)");
        tenv.executeSql("insert into iceberg_insert2(id, data1) values(1,3)");
        tenv.executeSql("select * from iceberg_insert2 ").print();
    }

    @Test
    public void test2() throws InterruptedException {
        int random = (int)(Math.random() * 1000) + 10;
        /*int random2 = (int)(Math.random() * 1000) + 10;
        System.out.println("***************** " + random + " " + random2);*/
        System.out.println("***************** " + random);
        tenv.executeSql("insert into iceberg_insert(id,data1) values(1,"+random+")");
        Thread.sleep(10000);
        tenv.executeSql("select * from iceberg_insert ").print();
    }

    /**
     * 构造长sql语句
     */
    @Test
    public void testMultipleLines() throws InterruptedException {
        StringBuilder str = new StringBuilder();
        String sql = "insert into iceberg_insert values";
        for(int i = 0; i < 1000; i++){
            int id= i;
            int data1 = i + 1;
            int data2 = i + 2;
            str.append("("+id+","+data1+","+data2+")");
            str.append(",");
        }
        String str2 = str.toString();
        str2 = str2.substring(0, str2.length() - 1);
        sql = sql + str2;
        System.out.println(sql);
        tenv.executeSql(sql);
        Thread.sleep(6000);
        tenv
            .executeSql("select * from iceberg_insert /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/")
            .print();
    }

    @Test
    public void testMultipleLines_insert2() throws InterruptedException {
        StringBuilder str = new StringBuilder();
        String sql = "insert into iceberg_insert(id,data1) values";
        for(int i = 0; i < 1000; i++){
            int id= i;
            int data1 = (int)Math.random() * 10000;
            str.append("("+id+","+data1+")");
            str.append(",");
        }
        String str2 = str.toString();
        str2 = str2.substring(0, str2.length() - 1);
        sql = sql + str2;
        System.out.println(sql);
        tenv.executeSql(sql);
        Thread.sleep(10000);
        tenv
                .executeSql("select * from iceberg_insert /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/")
                .print();
    }

    @Test
    public void testMultipleLines_insert() throws InterruptedException {
        StringBuilder str = new StringBuilder();
        String sql = "insert into iceberg_insert(id,data2) values(0,0)";
//        String sql = "insert into iceberg_insert(id,data1) values(0,0)";
        tenv.executeSql(sql);
        Thread.sleep(10000);
        tenv
                .executeSql("select * from iceberg_insert /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s')*/")
                .print();
    }

    @Test
    public void test3() throws Exception {
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "merge_test001"));
        //tenv.executeSql("insert into iceberg_insert(id,data1) values(1,10000)");
        TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
        DataStream<RowData> dataStream = FlinkSource.forRowData()
                .env(env)
                .streaming(false)
                .tableLoader(tableLoader)
                .build();
        dataStream.print();
        env.execute("213213");
    }

    @Test
    public void officaltest1Open() throws IOException, InterruptedException {
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert3(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50','write.upsert-part.enable'='true')");

        tenv.executeSql("insert into iceberg_insert3 values(1,2,3),(2,3,4),(3,4,5)");
        Thread.sleep(10000);
        tenv.executeSql("insert into iceberg_insert3(id, data1) values(1,3),(2,4),(3,5)");

        Thread.sleep(10000);
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_insert3"));
        // Assert the table records as expected.
        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 3,3),
                createRecord(2, 4,4),
                createRecord(3, 5,5)
        ));
        tenv.executeSql("drop table iceberg_insert3");
    }
    @Test
    public void officaltest2Close() throws IOException, InterruptedException {
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_insert4(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true'," +
                "'write.metadata.previous-versions-max'='5','flink.max-continuous-empty-commits'='50')");

        tenv.executeSql("insert into iceberg_insert4 values(1,2,3),(2,3,4),(3,4,5)");
        tenv.executeSql("insert into iceberg_insert4(id, data1) values(1,3),(2,4),(3,5)");

        Thread.sleep(3000);
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_insert4"));
        // Assert the table records as expected.
        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 3,null),
                createRecord(2, 4,null),
                createRecord(3, 5,null)
        ));
        tenv.executeSql("drop table iceberg_insert4");
    }


    public static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data1", Types.IntegerType.get()),
            optional(3, "data2", Types.IntegerType.get()));
    public static final Record RECORD = GenericRecord.create(SCHEMA);

    public static Record createRecord(Integer id, Integer data1, Integer data2) {
        Record record = RECORD.copy();
        record.setField("id", id);
        record.setField("data1", data1);
        record.setField("data2", data2);
        return record;
    }

    public static void main(String[] args) {

    }
}

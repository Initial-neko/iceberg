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
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestUpsert extends FlinkCatalogTestBase {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            MiniClusterResource.createWithClassloaderCheckDisabled();

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final String TABLE_NAME = "test_table";
    private StreamTableEnvironment tEnv;
    private Table icebergTable;

    @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = Lists.newArrayList();

        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
            String catalogName = (String) catalogParams[0];
            Namespace baseNamespace = (Namespace) catalogParams[1];
            parameters.add(new Object[]{catalogName, baseNamespace});
        }
        return parameters;
    }

    public TestUpsert(String catalogName, Namespace baseNamespace) {
        super(catalogName, baseNamespace);
    }

    @Override
    protected TableEnvironment getTableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
                        .newInstance()
                        .useBlinkPlanner();
                settingsBuilder.inStreamingMode();
                StreamExecutionEnvironment env = StreamExecutionEnvironment
                        .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                env.enableCheckpointing(10000);
                env.setMaxParallelism(2);
                env.setParallelism(2);
                tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
                org.apache.flink.configuration.Configuration configuration = tEnv.getConfig()
                        .getConfiguration();
                configuration.setString("table.dynamic-table-options.enabled", "true");

            }
        }
        return tEnv;
    }

    @Before
    public void before() {
        super.before();
        sql("CREATE DATABASE %s", flinkDatabase);
        sql("USE CATALOG %s", catalogName);
        sql("USE %s", DATABASE);
        sql("CREATE TABLE %s (id int, data1 int, data2 int ,PRIMARY KEY(id) NOT ENFORCED) " +
                "with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')", TABLE_NAME);
        icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    }

    @After
    public void clean() {
        sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
        sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
        super.clean();
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

    @Test
    public void TestWithTwoInsert() throws IOException, InterruptedException {

        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

        sql("insert into %s(id, data1) values(1,8),(2,9),(3,10)", TABLE_NAME);

        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 8,3),
                createRecord(2, 9,4),
                createRecord(3, 10,5)
        ));

        sql("insert into %s(id, data2) values(1,30),(2,40),(3,50)", TABLE_NAME);

        System.out.println(SnapshotUtil.currentAncestors(table));
        // Assert the table records as expected.
        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 8,30),
                createRecord(2, 9,40),
                createRecord(3, 10,50)
        ));

    }

    @Test
    public void TestWithOneInsertChangeTwice() throws Exception {

        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

//        sql("insert into %s(id, data1) values(1,8),(1,9),(2,10),(2,11),(3,23)", TABLE_NAME);

//        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
//                createRecord(1, 9, 3),
//                createRecord(2, 11, 4),
//                createRecord(3, 23, 5)
//        ));

        sql("insert into %s(id, data2) values(1,30),(1,40),(2,50),(2,60),(3,12),(3,13)", TABLE_NAME);

        // Assert the table records as expected.
//        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
//                createRecord(1, 9, 40),
//                createRecord(2, 11, 60),
//                createRecord(3, 23, 13)
//        ));

//        tEnv.executeSql(
//                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/",
//                        TABLE_NAME));
        org.apache.flink.table.api.Table sqlQuery = tEnv.sqlQuery(String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/",
                TABLE_NAME));
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(sqlQuery);
        StreamExecutionEnvironment env = rowDataStream.getExecutionEnvironment();
        rowDataStream.print();
        env.execute();

    }

    @Test
    public void TestWithOneInsertChangeTwiceBatch() throws Exception {

        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

//        sql("insert into %s(id, data1) values(1,8),(1,9),(2,10),(2,11),(3,23)", TABLE_NAME);

//        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
//                createRecord(1, 9, 3),
//                createRecord(2, 11, 4),
//                createRecord(3, 23, 5)
//        ));

//        sql("insert into %s(id, data2) values(1,30),(1,40),(2,50),(2,60),(3,12),(3,13)", TABLE_NAME);
        sql("insert into %s(id, data2) values(1,30),(1,40),(1,50),(1,60),(1,12),(1,13)", TABLE_NAME);

        // Assert the table records as expected.
//        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
//                createRecord(1, 9, 40),
//                createRecord(2, 11, 60),
//                createRecord(3, 23, 13)
//        ));

//        tEnv.executeSql(
//                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/",
//                        TABLE_NAME));
        org.apache.flink.table.api.Table sqlQuery = tEnv.sqlQuery(String.format("select * from %s /*+ OPTIONS('streaming'='true')*/",
                TABLE_NAME));
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(sqlQuery);
        StreamExecutionEnvironment env = rowDataStream.getExecutionEnvironment();
        rowDataStream.print();
        env.execute();

    }

}

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
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
public class TestStreamBatchConsume extends FlinkCatalogTestBase {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            MiniClusterResource.createWithClassloaderCheckDisabled();

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final String TABLE_NAME = "test_table";
    private TableEnvironment tEnv;
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

    public TestStreamBatchConsume(String catalogName, Namespace baseNamespace) {
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

    //batch mode can't process overwrite
    @Test
    public void TestWithPrimaryBatch() throws Exception {
        sql("CREATE TABLE %s (id int, data1 int, data2 int, PRIMARY KEY(id) NOT ENFORCED) " +
                "with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')", TABLE_NAME);

    }

    @Test
    public void TestWithNonPrimaryBatch() throws Exception {

        sql("CREATE TABLE %s (id int, data1 int, data2 int) " +
                "with('format-version' = '2')", TABLE_NAME);
        /*GenericAppenderHelper helper = new GenericAppenderHelper(table, FileFormat.PARQUET, TEMPORARY_FOLDER);

        List<Record> records1 = RandomGenericData.generate(SCHEMA, 1, 0L);
        helper.appendToTable(records1);
        long snapshotId1 = table.currentSnapshot().snapshotId();

        // snapshot 2
        List<Record> records2 = RandomGenericData.generate(SCHEMA, 1, 0L);
        helper.appendToTable(records2);

        List<Record> records3 = RandomGenericData.generate(SCHEMA, 1, 0L);
        helper.appendToTable(records3);
        long snapshotId3 = table.currentSnapshot().snapshotId();

        // snapshot 4
        helper.appendToTable(RandomGenericData.generate(SCHEMA, 1, 0L));

        List<Record> expected2 = Lists.newArrayList();
        expected2.addAll(records2);
        expected2.addAll(records3);
        TestHelpers.assertRecords(runWithOptions(
                ImmutableMap.<String, String>builder()
                        .put("streaming", "false")
                        .put("start-snapshot-id", Long.toString(snapshotId1))
                        .put("end-snapshot-id", Long.toString(snapshotId3)).build()),
                expected2, SCHEMA);*/
        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

        // snapshot 2
        sql("insert into %s values(1,3,5),(2,4,6),(3,5,7)", TABLE_NAME);

        sql("insert into %s values(4,5,6),(5,6,7),(6,7,8)", TABLE_NAME);

        // snapshot 4
        sql("insert into %s(id, data1) values(4,100),(5,200),(6,300)", TABLE_NAME);

        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        long snapshotId1 = ancestors.get(ancestors.size() - 1);
        long snapshotId3 = ancestors.get(ancestors.size() - 3);
        long snapshotId4 = ancestors.get(ancestors.size() - 4);

        List<Record> expected2 = Lists.newArrayList(
                createRecord(1, 3,5),
                createRecord(2, 4,6),
                createRecord(3, 5,7),

                createRecord(4, 5,6),
                createRecord(5, 6,7),
                createRecord(6, 7,8)
        );
        TestHelpers.assertRecords(runWithOptions(
                ImmutableMap.<String, String>builder()
                        .put("streaming", "false")
                        .put("start-snapshot-id", Long.toString(snapshotId1))
                        .put("end-snapshot-id", Long.toString(snapshotId3)).build()),
                expected2, SCHEMA);


        List<Record> expected3 = Lists.newArrayList(
                createRecord(4, 100,null),
                createRecord(5, 200,null),
                createRecord(6, 300,null)
        );
        TestHelpers.assertRecords(runWithOptions(
                ImmutableMap.<String, String>builder()
                        .put("streaming", "false")
                        .put("start-snapshot-id", Long.toString(snapshotId3))
                        .put("end-snapshot-id", Long.toString(snapshotId4)).build()),
                expected3, SCHEMA);

    }

    @Test
    public void TestWithPrimaryStream() throws Exception {

        sql("CREATE TABLE %s (id int, data1 int, data2 int, PRIMARY KEY(id) NOT ENFORCED) " +
                "with('format-version' = '2','write.upsert.enable'='true','write.upsert-part.enable'='true')", TABLE_NAME);

//        icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

        // snapshot 2
        sql("insert into %s values(1,3,5),(2,4,6),(3,5,7)", TABLE_NAME);

        sql("insert into %s values(4,5,6),(5,6,7),(6,7,8)", TABLE_NAME);

        // snapshot 4
        sql("insert into %s(id, data1) values(4,100),(5,200),(6,300)", TABLE_NAME);

        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        long snapshotId1 = ancestors.get(ancestors.size() - 1);
        long snapshotId3 = ancestors.get(ancestors.size() - 3);
        long snapshotId4 = ancestors.get(ancestors.size() - 4);

        //需要手动执行观察情况，流测试不知道如何返回时间结果，之后可以再补充成那样的单元测试
        tEnv.executeSql(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'start-snapshot-id'='%s', 'monitor-interval'='10s')*/",
                        TABLE_NAME, Long.toString(snapshotId1))).print();
//        sql("select * from %s /*+ OPTIONS('streaming'='true', 'start-snapshot-id'='%s', 'monitor-interval'='5s')*/", TABLE_NAME, Long.toString(snapshotId3));

    }

    @Test
    public void TestWithNonPrimaryStream() throws Exception {

        sql("CREATE TABLE %s (id int, data1 int, data2 int) " +
                "with('format-version' = '2')", TABLE_NAME);

        Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

        sql("insert into %s values(1,2,3),(2,3,4),(3,4,5)", TABLE_NAME);

        // snapshot 2
        sql("insert into %s values(1,3,5),(2,4,6),(3,5,7)", TABLE_NAME);

        sql("insert into %s values(4,5,6),(5,6,7),(6,7,8)", TABLE_NAME);

        // snapshot 4
        sql("insert into %s(id, data1) values(4,100),(5,200),(6,300)", TABLE_NAME);

        table.refresh();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        long snapshotId1 = ancestors.get(ancestors.size() - 1);
        long snapshotId3 = ancestors.get(ancestors.size() - 3);
        long snapshotId4 = ancestors.get(ancestors.size() - 4);

        tEnv.executeSql(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'start-snapshot-id'='%s', 'monitor-interval'='5s')*/",
                        TABLE_NAME, Long.toString(snapshotId1))).print();


    }


    protected List<Row> runWithOptions(Map<String, String> options) throws Exception {
        FlinkSource.Builder builder = FlinkSource.forRowData();
        Optional.ofNullable(options.get("streaming")).ifPresent(value -> builder.streaming(Boolean.parseBoolean(value)));
        Optional.ofNullable(options.get("snapshot-id")).ifPresent(value -> builder.snapshotId(Long.parseLong(value)));
        Optional.ofNullable(options.get("start-snapshot-id"))
                .ifPresent(value -> builder.startSnapshotId(Long.parseLong(value)));
        Optional.ofNullable(options.get("end-snapshot-id"))
                .ifPresent(value -> builder.endSnapshotId(Long.parseLong(value)));
        Optional.ofNullable(options.get("as-of-timestamp"))
                .ifPresent(value -> builder.asOfTimestamp(Long.parseLong(value)));
        return run(builder, options, "", "*");
    }


    protected List<Row> run() throws Exception {
        return run(FlinkSource.forRowData(), Maps.newHashMap(), "", "*");
    }

    protected List<Row> run(FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter,
                            String... sqlSelectedFields) {
        String select = String.join(",", sqlSelectedFields);

        StringBuilder builder = new StringBuilder();
        sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

        String optionStr = builder.toString();

        if (optionStr.endsWith(",")) {
            optionStr = optionStr.substring(0, optionStr.length() - 1);
        }

        if (!optionStr.isEmpty()) {
            optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
        }

        return sql("select %s from %s %s %s", select, TABLE_NAME, optionStr, sqlFilter);
    }

    protected void runStream(FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter,
                            String... sqlSelectedFields) {
        String select = String.join(",", sqlSelectedFields);

        StringBuilder builder = new StringBuilder();
        sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

        String optionStr = builder.toString();

        if (optionStr.endsWith(",")) {
            optionStr = optionStr.substring(0, optionStr.length() - 1);
        }

        if (!optionStr.isEmpty()) {
            optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
        }

        sql("select %s from %s %s %s", select, TABLE_NAME, optionStr, sqlFilter);
    }

    private String optionToKv(String key, Object value) {
        return "'" + key + "'='" + value + "'";
    }

}

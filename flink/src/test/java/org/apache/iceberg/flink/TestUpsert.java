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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
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
    private TableEnvironment tEnv;
    private Table icebergTable;

    private final FileFormat format;
    private final boolean isStreamingJob;

    @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = Lists.newArrayList();
        for (FileFormat format : new FileFormat[]{FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
            for (Boolean isStreaming : new Boolean[]{true, false}) {
                for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
                    String catalogName = (String) catalogParams[0];
                    Namespace baseNamespace = (Namespace) catalogParams[1];
                    parameters.add(new Object[]{catalogName, baseNamespace, format, isStreaming});
                }
            }
        }
        return parameters;
    }

    public TestUpsert(String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
        super(catalogName, baseNamespace);
        this.format = format;
        this.isStreamingJob = isStreamingJob;
    }

    @Override
    protected TableEnvironment getTableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
                        .newInstance()
                        .useBlinkPlanner();
                if (isStreamingJob) {
                    settingsBuilder.inStreamingMode();
                    StreamExecutionEnvironment env = StreamExecutionEnvironment
                            .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
                    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    env.enableCheckpointing(400);
                    env.setMaxParallelism(2);
                    env.setParallelism(2);
                    tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
                } else {
                    settingsBuilder.inBatchMode();
                    tEnv = TableEnvironment.create(settingsBuilder.build());
                }
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
        sql("CREATE TABLE %s (id int, data1 int, data2 int) " +
                "with ('write.format.default'='%s','write.upsert-part.enable'='true')", TABLE_NAME, format.name());
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

        sql("insert into %s((id, data1) values(1,8),(2,9),(3,10)", TABLE_NAME);

        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 8,3),
                createRecord(2, 9,4),
                createRecord(3, 10,5)
        ));

        sql("insert into %s((id, data2) values(1,30),(2,40),(3,50)", TABLE_NAME);

        // Assert the table records as expected.
        SimpleDataUtil.assertTableRecords(table, Lists.newArrayList(
                createRecord(1, 8,30),
                createRecord(2, 9,40),
                createRecord(3, 10,50)
        ));

    }

}

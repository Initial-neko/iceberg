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


package org.apache.iceberg.flink.actions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.sun.rowset.internal.InsertRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.RECORD;

public class TestOverwrite {

    static StreamExecutionEnvironment env = null;
    static StreamTableEnvironment tenv = null;
    static String database = "hdp_teu_dpd_default_stream_db1";
    static String tableName = "test_inc";

    public static HiveCatalog getCatalog() {
        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hive");
        config.put("uri", "thrift://10.162.12.69:9083");
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog");
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.initialize("iceberg_hadoop_catalog", config);
        return hiveCatalog;
    }

    @Test
    public void clean(){
        HiveCatalog catalog = getCatalog();
        List<TableIdentifier> default_stream_db1 = catalog.listTables(Namespace.of("hdp_teu_dpd_default_stream_db1"));
        for(int i = 0; i < default_stream_db1.size(); i++){
            if(default_stream_db1.get(i).name().contains(tableName)){
                System.out.println(default_stream_db1.get(i).name());
            }
        }
    }

    @Before
    public static void init(){
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
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();
        configuration.setString("execution.type", "batch");
        configuration.setString("table.dynamic-table-options.enabled", "true");
    }


    public static void main(String[] args) throws InterruptedException {
        init();

        tenv.useCatalog("iceberg_hadoop_catalog");
        tenv.useDatabase("hdp_teu_dpd_default_stream_db1");

        tableName = tableName + (int)(Math.random() * 10000 + 1);
        tenv.executeSql("CREATE TABLE IF NOT EXISTS "+tableName+"(" +
                "  `id`  INT NOT NULL," +
                "  `data1`   INT NOT NULL," +
                "  `data2`   INT," +
                "  PRIMARY KEY(id) NOT ENFORCED" +
                ") with('format-version' = '2','write.upsert.enabled'='true')");

        HiveCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, tableName));

        table.updateSpec().addField("data1").commit();
        table.updateSchema().setIdentifierFields("id", "data1").commit();
        System.out.println(table.specs());

        tenv.executeSql("insert into "+tableName+" values(1,2,3),(2,3,4),(3,4,5)");
        Thread.sleep(3000);
        tenv.executeSql("insert into "+tableName+" values(1,2,4),(2,3,5),(3,4,6)");
        Thread.sleep(3000);
        /*tenv.executeSql("insert into "+tableName+" values(1,2,6),(2,3,7),(3,4,8)");
        Thread.sleep(3000);*/
        table.refresh();
        table.currentSnapshot().deleteManifests().forEach(System.out::println);
        table.currentSnapshot().dataManifests().forEach(System.out::println);

        table.newDelete().deleteFromRowFilter(Expressions.equal("data1",3)).commit();
//        table.newScan().planFiles().forEach(System.out::println);

        table.refresh();
        table.currentSnapshot().deleteManifests().forEach(System.out::println);
        table.currentSnapshot().dataManifests().forEach(System.out::println);

        table.snapshots().forEach(System.out::println);

        tenv.executeSql("insert into "+tableName+" values(1,2,6),(2,3,7),(3,4,8)");
        Thread.sleep(3000);
        table.newScan().planFiles().forEach(System.out::println);

        table.refresh();


//        tenv.executeSql("select * from "+tableName).print();
    }

    protected static Row insertRow(Object... values) {
        return Row.ofKind(RowKind.INSERT, values);
    }

    protected static Row deleteRow(Object... values) {
        return Row.ofKind(RowKind.DELETE, values);
    }

    protected static Row updateBeforeRow(Object... values) {
        return Row.ofKind(RowKind.UPDATE_BEFORE, values);
    }

    protected static Row updateAfterRow(Object... values) {
        return Row.ofKind(RowKind.UPDATE_AFTER, values);
    }

}

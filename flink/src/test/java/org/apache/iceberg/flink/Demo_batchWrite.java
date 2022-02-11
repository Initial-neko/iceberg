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

public class Demo_batchWrite {

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
        env.enableCheckpointing(50000);

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
    public void TestEndInput() throws Exception {

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
        /*tenv.executeSql(String.format("insert into %s values(1,2,3),(1,3,4),(1,4,5),(1,5,7),(1,100,200)", TABLE_NAME));
        Thread.sleep(5000);*/
        tenv.executeSql(String.format("insert into %s(id, data1) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);
        /*tenv.executeSql(String.format("insert into %s(id, data2) values(1,3),(1,5),(1,7)", TABLE_NAME));
        Thread.sleep(5000);*/

        //sql can add "start-snapshot-id"
        org.apache.flink.table.api.Table sqlQuery = tenv.sqlQuery(
                String.format("select * from %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='10s')*/", TABLE_NAME));

        DataStream<Row> rowDataStream = tenv.toChangelogStream(sqlQuery);
        rowDataStream.print();
        env.execute("test with sql datastream");
    }


}

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

package org.apache.iceberg;

import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

public class MySparkTest {

    SparkSession spark = null;

    HiveCatalog catalog = null;

    String database = "hdp_teu_dpd_default_stream_db1";

    public HiveCatalog getCatalog() {
        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hive");
        config.put("uri", "thrift://10.162.12.69:9083");
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog");
        HiveCatalog hiveCatalog = new HiveCatalog(new Configuration());
        hiveCatalog.initialize("dataplat_hadoop_catalog", config);
        return hiveCatalog;
    }

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        /*val conf = new Configuration
        conf.addResource("test/core-site.xml")
        conf.addResource("test/hdfs-site.xml")
        conf.addResource("test/mountTable.xml")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")*/
        SparkConf sparkConf = new SparkConf();
        spark = SparkSession.builder()
                .appName(this.getClass().getSimpleName())
                .config("spark.sql.catalog.dataplat_hadoop_catalog.type", "hive").config(sparkConf)
                .config("spark.sql.catalog.dataplat_hadoop_catalog", SparkCatalog.class.getName())
                .config("spark.sql.catalog.dataplat_hadoop_catalog.uri", "thrift://10.162.12.69:9083")
                .config("spark.sql.catalog.dataplat_hadoop_catalog.warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog")
//      .config("spark.sql.catalog.dataplat_hadoop_catalog.warehouse", "hdfs://hdp-test/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog")
//                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .master("local[*]")
                .getOrCreate();
//        catalog = IcebergUtils.getHiveCatalogTest(conf)
        catalog = getCatalog();
        spark.sql("use dataplat_hadoop_catalog");
        spark.sql("use hdp_teu_dpd_default_stream_db1");
    }

    @Test
    public void test(){
//        spark.sql("show tables").show(false);
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_insert2"));
        table.snapshots().forEach(t -> System.out.println(t.snapshotId()));
        spark
                .read()
                .format("iceberg")
                .option("start-snapshot-id", 1113766094827150024L)
                .option("end-snapshot-id", 7026424359006321179L)
                .table(table.name()).show(false);
    }
}

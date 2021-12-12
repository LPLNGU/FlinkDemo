package hbaseredistest;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * @program: WordCountDemo
 * @description: 测试三级缓存
 * @author: 李沛隆21081020
 * @create: 2021-11-17 17:51
 */
public class HbaseRedisCacheTest {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);

        tableEnv.executeSql("CREATE TABLE test_1_11_1_7 (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://10.243.59.137:3306/test12',\n" +
                "   'table-name' = 'test_1_11_1_7', \n" +
                "   'username' = 'fabu', \n" +
                "   'password' = '00pDcK1K5exU', \n" +
                "   'zkUrl' = '10.243.67.157:2015', \n" +
                "   'redis.cache.url' = '10.243.67.157:6379', \n" +
                "   'lookup.cache.max-rows' = '2', \n" +
                "   'lookup.cache.ttl' = '5000' \n" +
                ")");

        tableEnv.executeSql("CREATE TABLE kafkaTable (" +
                " id BIGINT," +
                " name STRING," +
                " ts AS PROCTIME()" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'jchen_test1'," +
                " 'properties.bootstrap.servers' = '10.243.67.158:9092'," +
                " 'format' = 'csv'," +
                " 'csv.ignore-parse-errors' = 'true', " +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")");

        tableEnv.executeSql("CREATE TABLE kafkaTable2 (" +
                " id BIGINT," +
                " name STRING," +
                " ts TIMESTAMP(3)," +
                " bid BIGINT," +
                " bname STRING" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'jchen_test2'," +
                " 'properties.bootstrap.servers' = '10.243.67.158:9092'," +
                " 'format' = 'csv'," +
                " 'csv.ignore-parse-errors' = 'true', " +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")");

//        tableEnv.executeSql("insert into test_1_11_1_7 select * from kafkaTable /*+ OPTIONS('properties.group.id'='flink_test_1_11_1_7') */ ");
        TableResult tableResult = tableEnv.executeSql(" insert into kafkaTable2 " +
                " SELECT A.id, A.name, A.ts, B.id as bid, B.name as bname FROM kafkaTable /*+ OPTIONS('properties.group.id'='flink_test_1_11_1_7') */ as A  \n" +
                " LEFT JOIN test_1_11_1_7 FOR SYSTEM_TIME AS OF A.ts AS B \n" +
                " ON A.id = B.id");
    }

}

package tablesql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 20017959
 * @version 1.0
 * @description: 测试维表关联
 * @date 2021/11/29 10:58
 */
public class TestCache {

    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000); // 10s 做一个checkpoint

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 每次Query时动态指定或覆写表的option,
        // 需要加上配置set table.dynamic-table-options.enabled=true来开启，默认关闭
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
//                "   'zkUrl' = '10.243.67.157:2015', \n" +
//                "   'redis.cache.url' = '10.243.67.157:6379', \n" +
                "   'lookup.cache.max-rows' = '2', \n" +
                "   'lookup.cache.ttl' = '5000' \n" +
                ")");


        tableEnv.executeSql("CREATE TABLE kafkaTable (" +
                " id BIGINT," +
                " name STRING," +
                " ts AS PROCTIME()" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'hsj'," +
                " 'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092'," +
                " 'format' = 'csv'," +
                " 'csv.ignore-parse-errors' = 'true', " +
                " 'scan.startup.mode' = 'latest-offset'" +
                ")");

//        tableEnv.executeSql("CREATE TABLE kafkaTable2 (" +
//                " id BIGINT," +
//                " name STRING," +
//                " ts TIMESTAMP(3)," +
//                " bid BIGINT," +
//                " bname STRING" +
//                ") WITH (" +
//                " 'connector' = 'kafka'," +
//                " 'topic' = 'jchen_test2'," +
//                " 'properties.bootstrap.servers' = '10.243.67.158:9092'," +
//                " 'format' = 'csv'," +
//                " 'csv.ignore-parse-errors' = 'true', " +
//                " 'scan.startup.mode' = 'earliest-offset'" +
//                ")");

//        tableEnv.executeSql("insert into test_1_11_1_7 select * from kafkaTable /*+ OPTIONS('properties.group.id'='flink_test_1_11_1_7') */ ");



//        TableResult tableResult2 = tableEnv.executeSql(
//                "select * from test_1_11_1_7");
//        tableResult2.print();

//            TableResult tableResult = tableEnv.executeSql(
//                    " SELECT A.id, A.name, A.ts, B.id as bid, B.name as bname FROM kafkaTable  as A  \n" +
//                            " LEFT JOIN test_1_11_1_7 FOR SYSTEM_TIME AS OF A.ts AS B \n" +
//                            " ON A.id = B.id");
//            tableResult.print();


        tableEnv.executeSql("select * from kafkaTable").print();


    }

}

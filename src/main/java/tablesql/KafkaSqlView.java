package tablesql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: WordCountDemo
 * @description: 用于测试kafka流表view debug
 * @author: 李沛隆21081020
 * @create: 2022-02-10 15:29
 */
public class KafkaSqlView {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        env.setParallelism(2);
        String kafkaTable = "CREATE TABLE SourceA (\n" +
                " id STRING,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'hsj-sink1',\n" +
                " 'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092',\n" +
//                " 'properties.group.id' = 'lpl',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        String printTable = "create table print (\n" +
                "  id    STRING,\n" +
                "  name  STRING\n" +
                ") with (\n" +
                "  'connector' = 'print'\n" +
                ")";
        String tableView = "create view test_view as\n" +
                "select\n" +
                "  *\n" +
                "from SourceA /*+ OPTIONS('properties.group.id'='lpl') */";
        String insert = "insert into print select * from test_view";
        //测试view
        String viewSelectTest = "select * from SourceA /*+ OPTIONS('properties.group.id'='lpl') */";


        tEnv.executeSql(kafkaTable);
        tEnv.executeSql(printTable);
        //for debugging work flow
        tEnv.executeSql(viewSelectTest);
        //tEnv.executeSql(tableView);
        tEnv.executeSql(insert);
    }
}

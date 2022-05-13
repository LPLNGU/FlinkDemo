package hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: WordCountDemo
 * @description: 测试Hbase sql
 * @author: 李沛隆21081020
 * @create: 2022-03-30 11:28
 */
public class HbaseSqlTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        env.setParallelism(2);
        String hbaseTable = "CREATE TABLE hTable (\n" +
                " rowkey VARCHAR,\n" +
                " f ROW<name VARCHAR>\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'ns_test:test',\n" +
                " 'zookeeper.quorum' = '10.27.1.138:2015'" +
                ")";
//        String insert = "insert into hTable " +
//                "VALUES('111222333',1222233111)";
        String scanTable =
                "select\n" +
                        "  rowkey,\n" +
                        "  f.name\n" +
                        "from hTable";
//                        "  where rowkey = 'fff8135405e74c7ea5f9110426114b6d'";

        tEnv.executeSql(hbaseTable);
        //for debugging work flow
//        tEnv.executeSql(insert);
        TableResult tableResult = tEnv.executeSql(scanTable);
        tableResult.print();
    }
}

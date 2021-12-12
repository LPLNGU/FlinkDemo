package tablesql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: lipeilong
 * @Date: 2021/9/7 16:34
 */
public class RegularJoinWithSQLDemo {


    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        env.setParallelism(1);

        // 流1
        // {"userID":"user_1","eventType":"browse","eventTime":"2015-01-01 00:00:00"}
        String kafkaBrowse = "CREATE TABLE kafka_stream_01 (\n" +
                " userID STRING,\n" +
                " eventType STRING,\n" +
                " eventTime STRING,\n" +
                " proctime as PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'hsj',\n" +
                " 'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092',\n" +
                " 'properties.group.id' = 'hsj123',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka_stream_01");
        tableEnv.executeSql(kafkaBrowse);
        Table result1 = tableEnv.sqlQuery("select * from kafka_stream_01");
        tableEnv.toRetractStream(result1, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.SQL_TIMESTAMP)).print();
//
//        // 流2 -- hsj-sink1
//        // {"userID":"user_1","userName":"name1","userAge":10,"userAddress":"Mars"}
//        String kafkaUser = ""
//                + "CREATE TABLE kafka_stream_02 "
//                + "( "
//                + "    userID STRING, "
//                + "    userName STRING, "
//                + "    userAge INT, "
//                + "    userAddress STRING "
//                + ") WITH ( "
//                + "    'connector' = 'kafka', "
//                + "    'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092," +
//                "kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092 ', "
//                + "    'topic' = 'hsj-sink1', "
//                + "    'properties.group.id' = 'BD_hsj-sink1', "
//                + "    'scan.startup.mode' = 'latest-offset', "
//                + "    'format' = 'json' "
//                + ")";
//        tableEnv.executeSql("DROP TABLE IF EXISTS kafka_stream_02");
//        tableEnv.executeSql(kafkaUser);
//        Table result2 = tableEnv.sqlQuery("select * from kafka_stream_02");
//        tableEnv.toRetractStream(result2, Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.STRING)).print();
//
//        // INNER JOIN
//        //String execSQL = ""
//        //        + "SELECT * "
//        //        + "FROM kafka_browse_log "
//        //        + "INNER JOIN kafka_user_change_log "
//        //        + "ON kafka_browse_log.userID = kafka_user_change_log.userID ";
//        //tableEnv.toAppendStream(tableEnv.sqlQuery(execSQL), Row.class).print();
//
//        // LEFT JOIN
//        // String execSQL = ""
//        //         + "SELECT kafka_stream_01.userID, kafka_stream_02.userName "
//        //         + "FROM kafka_stream_01 "
//        //         + "LEFT JOIN kafka_stream_02 "
//        //       + "ON kafka_stream_01.userID = kafka_stream_02.userID "
//        //       + "GROUP BY kafka_stream_01.userID, kafka_stream_02.userName ";
//        String execSQL = ""
//                + "SELECT * "
//                + "FROM kafka_stream_01 "
//                + "LEFT JOIN kafka_stream_02 "
//                + "ON kafka_stream_01.userID = kafka_stream_02.userID ";
//        Table result = tableEnv.sqlQuery(execSQL);
//
//        // tableEnv.toRetractStream(result,Row.class).print();
//        tableEnv.toAppendStream(result,
//                Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.SQL_TIMESTAMP,
//                        Types.STRING, Types.STRING, Types.INT, Types.STRING)).print();
//
//        // RIGHT JOIN
//        // String execSQL = ""
//        //         + "SELECT * "
//        //         + "FROM kafka_browse_log "
//        //         + "RIGHT JOIN kafka_user_change_log "
//        //         + "ON kafka_browse_log.userID = kafka_user_change_log.userID ";
//        // tableEnv.toRetractStream(tableEnv.sqlQuery(execSQL), Row.class).print();
//
//        // FULL JOIN
//        // String execSQL = ""
//        //         + "SELECT * "
//        //         + "FROM kafka_browse_log "
//        //         + "FULL JOIN kafka_user_change_log "
//        //         + "ON kafka_browse_log.userID = kafka_user_change_log.userID ";
//        // tableEnv.toRetractStream(tableEnv.sqlQuery(execSQL), Row.class).print();

        env.execute();
    }
}

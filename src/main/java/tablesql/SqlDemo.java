package tablesql;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tablesql.bean.Order;

/**
 * @Author: lipeilong
 * @Date: 2021/8/26 11:54
 */
public class SqlDemo {


    public static void main(String[] args) throws Exception {

        //环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //table环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        //动态表
        tEnv.getConfig().getConfiguration()
                .setBoolean("table.dynamic-table-options.enabled", true);

        tEnv.executeSql(
                "CREATE TABLE orders (\n" +
                        " userId STRING,\n" +
                        " product STRING,\n" +
                        " amount INTEGER\n" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'hsj-sink1',\n" +
                        " 'properties.bootstrap.servers' = " +
                        "'kafkasitoltp01broker01.cnsuning.com:9092," +
                        "kafkasitoltp01broker02.cnsuning.com:9092," +
                        "kafkasitoltp01broker03.cnsuning.com:9092'," +
                        " 'properties.group.id' = 'BD_hsj-sink1',\n" +
                        " 'format' = 'json',\n" +
                        " 'scan.startup.mode' = 'latest-offset')"
        );

//        Table table = tEnv.sqlQuery("SELECT * FROM orders");
//        tEnv.toAppendStream(table, Order.class).print();

        tEnv.executeSql(
                "CREATE TABLE user_table (userId STRING, userName STRING) WITH (\n" +
                        "    'connector' = 'jdbc',\n" +
                        "    'url' = 'jdbc:mysql://10.243.59.137:3306/" +
                        "lpltest?useUnicode=true&characterEncoding=utf8',\n" +
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "    'table-name' = 'users',\n" +
                        "    'username' = 'fabu',\n" +
                        "    'password' = '00pDcK1K5exU'\n" +
                        ")");

//        join result table
        Table table = tEnv.sqlQuery(
                "SELECT\n" +
                        "    userName,\n" +
                        "    product,\n" +
                        "    amount\n" +
                        "FROM\n" +
                        "    orders,\n" +
                        "    user_table\n" +
                        "WHERE\n" +
                        "    orders.userId = user_table.userId");

        //groupby,使用retractStream对groupBy sql进行输出
//        Table table = tEnv.sqlQuery("SELECT userId,sum(amount) as boughtSum " +
//                "FROM orders group by userId");
//        tEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
//        })).print();


        DataStream<Tuple2<Boolean, Tuple3<String, String, Integer>>> resStream = tEnv
                .toRetractStream(table, TypeInformation
                        .of(new TypeHint<Tuple3<String, String, Integer>>() {
                        }));
        resStream.print();

        /*  data
            {"userId":"ddd","product":"apple","amount":1}
            {"userId":"111","product":"banana","amount":3}
            {"userId":"abc","product":"peer","amount":4}
         * */

        env.execute();

    }


}
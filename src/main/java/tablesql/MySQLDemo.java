package tablesql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQLDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);

        String preJudge = "Drop TABLE IF EXISTS testtable";
        String ddlSource =
                "CREATE TABLE testtable (\n" +
                        "  user_id INT,\n" +
                        "  item_id INT,\n" +
                        "  category_id INT,\n" +
                        "  behavior STRING ,\n" +
                        "  ts TIMESTAMP(3)\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc', -- 连接器\n" +
                        "  'driver'='com.mysql.cj.jdbc.Driver',\n" +
                        "  'username' = 'fabu',  --mysql用户名\n" +
                        "  'password' = '00pDcK1K5exU',  -- mysql密码\n" +
                        "  'table-name' = 'testtable',\n" +
                        "  'url' = 'jdbc:mysql://10.243.59.137:3306/lpltest?useUnicode=true&characterEncoding=utf8'\n" +
                        ")";
        tEnv.executeSql(preJudge);
        tEnv.executeSql(ddlSource);
        Table res = tEnv.sqlQuery("select * from testtable");
        res.printSchema();

        tEnv.toRetractStream(res, Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.STRING, Types.SQL_TIMESTAMP))
                .print();
        env.execute("execute");
    }
}

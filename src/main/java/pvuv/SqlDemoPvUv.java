package pvuv;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinkSQL计算PVUV的Demo
 *
 * @program: WordCountDemo
 * @author: 李沛隆21081020
 * @create: 2022-05-05 19:57
 */
public class SqlDemoPvUv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        env.setParallelism(2);
        //data_gen_source 用于生成流
        String sourceTable = "create table data_gen_source (\n" +
                "    id        int\n" +
                "    ,name     string\n" +
                "    ,sex      string\n" +
                "    ,age      int\n" +
                "    ,birthday string\n" +
                "    ,proc_time as proctime()\n" +
                ") with (\n" +
                "    'connector' = 'datagen'\n" +
                "    ,'rows-per-second' = '10000'\n" +
                "    ,'fields.id.kind' = 'random'\n" +
                "    ,'fields.id.min' = '1'\n" +
                "    ,'fields.id.max' = '2000000'\n" +
                ")";
        /*String print = "create table print_sink(\n" +
                "    start_time STRING\n" +
                "    ,end_time STRING\n" +
                "    ,pv  bigint\n" +
                "    ,uv  bigint\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                "    'logger' = 'true'" +
                ")";*/
        /*String insert = "INSERT INTO print_sink\n" +
                "SELECT date_format(window_start, 'HH:mm:ss') ,\n" +
                "       date_format(window_end, 'HH:mm:ss') ,\n" +
                "       count(id) ,\n" +
                "       count(DISTINCT id)\n" +
                "FROM TABLE(TUMBLE(TABLE data_gen_source, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))\n" +
                "GROUP BY window_start,\n" +
                "         window_end";*/
        tEnv.executeSql(sourceTable);
        /*tEnv.executeSql(print);*/
        /*tEnv.executeSql(insert);*/
        Table table = tEnv.sqlQuery(
                "SELECT TUMBLE_START(proc_time, INTERVAL '10' SECOND) ,\n" +
                "       TUMBLE_END(proc_time, INTERVAL '10' SECOND) ,\n" +
                "       count(id) ,\n" +
                "       count(DISTINCT id)\n" +
                "FROM data_gen_source\n" +
                "GROUP BY TUMBLE(proc_time,INTERVAL '10' SECOND)");
        tEnv.toRetractStream(table, Types.TUPLE(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.BIG_INT, Types.BIG_INT)).print();

        tEnv.execute("PV&UV Job");
    }
}

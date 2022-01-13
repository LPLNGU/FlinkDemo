package tablesql;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;

/**
 * @program: WordCountDemo
 * @description: 测试topN功能
 * @author: 李沛隆21081020
 * @create: 2022-01-04 09:20
 */
public class TopNTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//        env.enableCheckpointing(5000); // 10s 做一个checkpoint

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 每次Query时动态指定或覆写表的option,
        // 需要加上配置set table.dynamic-table-options.enabled=true来开启，默认关闭
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);

        TableResult tableResult = tableEnv.executeSql("CREATE TABLE kafkaTable (" +
                " id BIGINT," +
                " name STRING," +
                " number INTEGER," +
                " proc_time AS PROCTIME()" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'hsj-sink1'," +
                " 'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092," +
                "kafkasitoltp01broker02.cnsuning.com:9092," +
                "kafkasitoltp01broker03.cnsuning.com:9092'," +
                " 'format' = 'json'," +
                " 'scan.startup.mode' = 'latest-offset'" +
                ")");
        Table table = tableEnv.sqlQuery("SELECT *\n" +
                "\tFROM (\n" +
                "\t\tSELECT *,\n" +
                "\t\t\tROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num -- DESC表示降序\n" +
                "\t\tFROM (\n" +
                "\t\t\tSELECT id, sum(number) as icount,\n" +
                "\t\t\t\t\tTUMBLE_END(proc_time, INTERVAL '20' SECOND) as windowEnd\n" +
                "\t\t\t\t\tFROM kafkaTable GROUP BY id, TUMBLE(proc_time, INTERVAL '20' SECOND)\n" +
                "\t\t)\n" +
                "\t)\n" +
                "WHERE row_num <= 3");
        DataStream<Tuple2<Boolean, Tuple4<Long, Long, Timestamp, Long>>> retractStream =
                tableEnv.toRetractStream(table, TypeInformation.of(
                        new TypeHint<Tuple4<Long, Long, Timestamp, Long>>() {
                        })
                );


//        Table table = tableEnv.sqlQuery("SELECT id,count(number),TUMBLE_END(proc_time,INTERVAL '10' SECOND) as windowEnd FROM kafkaTable GROUP BY id,TUMBLE(proc_time,INTERVAL '10' SECOND)");
//        DataStream<Tuple2<Boolean, Tuple3<Long, Long, Timestamp>>> retractStream =
//                tableEnv.toRetractStream(table, TypeInformation.of(
//                        new TypeHint<Tuple3<Long, Long, Timestamp>>() {
//                        })
//                );
        retractStream.print();

        //data: {"id":1,"name":"1","number":5} {"id":1,"name":"1","number":6} {"id":1,"name":"1","number":77}
        env.execute();
    }
}

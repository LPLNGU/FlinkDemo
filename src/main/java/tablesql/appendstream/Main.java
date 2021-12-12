package tablesql.appendstream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @program: WordCountDemo
 * @description: 如果dynamic table只包含了插入新数据的操作那么就可以转化为append-only stream，所有数 据追加到stream里面
 * @author: 李沛隆21081020
 * @create: 2021-10-26 13:50
 */
public class Main {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, String>> data = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );
        Table table = stEnv.fromDataStream(
                data,
                $("name"),
                $("place")
        );
        stEnv.createTemporaryView("myTable", table);
        Table table1 = stEnv.sqlQuery("select name,place from myTable where name = 'Mary'");
        DataStream<Tuple2<String, String>> ds = stEnv.toAppendStream(table1, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        ds.print();
        env.execute("appendStream");
    }
}

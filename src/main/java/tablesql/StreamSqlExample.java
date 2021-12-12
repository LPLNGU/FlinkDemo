package tablesql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tablesql.bean.BoughtNum;
import tablesql.bean.Order;

import java.util.Arrays;

/**
 * @Author: lipeilong
 * @Date: 2021/9/7 15:45
 */
public class StreamSqlExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        DataStream<Order> orderDataStream1 = env.fromCollection(Arrays.asList(
                new Order("111", "cookie", 2),
                new Order("222", "pencil", 1),
                new Order("1111", "ball", 3)
        ));

        DataStream<Order> orderDataStream2 = env.fromCollection(Arrays.asList(
                new Order("222", "pencil", 2),
                new Order("111", "ball", 3),
                new Order("333", "ball", 1)
        ));


        Table order1 = streamTableEnv.fromDataStream(orderDataStream1);
        Table order2 = streamTableEnv.fromDataStream(orderDataStream2);

        streamTableEnv.createTemporaryView("order1", order1);
        streamTableEnv.createTemporaryView("order2", order2);

        /*//union
        Table table = streamTableEnv.sqlQuery("SELECT * FROM order1 WHERE amount > 2 " +
                "UNION ALL SELECT * FROM order2 WHERE amount < 2");*/
        //group by
        Table table = streamTableEnv.sqlQuery("SELECT user,SUM (amount) as boughtNum FROM order1 GROUP BY user");

        streamTableEnv.toRetractStream(table, BoughtNum.class).print();
        env.execute();
    }
}

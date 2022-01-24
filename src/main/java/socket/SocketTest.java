package socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @program: WordCountDemo
 * @description:
 * @author: 李沛隆21081020
 * @create: 2022-01-21 16:48
 */
public class SocketTest {

    public static void main(String[] args) throws Exception {
        // 创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.使用StreamExecutionEnvironment创建DataStream
        //Source(可以有多个Source)
        //Socket 监听本地端口8888
        // 接收一个socket文本流
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        // 3.进行转化处理统计
        //Transformation(s)对数据进行处理操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                //切分
                String[] words = line.split(" ");
                //循环，
                for (String word : words) {
                    //将每个单词与 1 组合，形成一个元组
                    Tuple2<String, Integer> tp = Tuple2.of(word, 1);
                    //将组成的Tuple放入到 Collector 集合，并输出
                    out.collect(tp);
                }
            }
        });

        //进行分组聚合(keyBy：将key相同的分到一个组中)
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = wordAndOne.keyBy(0).sum(1);

        //Transformation 结束

        //4.调用Sink （Sink必须调用）
        resultDataStream.print().setParallelism(1);

        //5. 启动任务执行
        env.execute("stream word count");
    }
}

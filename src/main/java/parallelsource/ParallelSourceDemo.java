package parallelsource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program: WordCountDemo
 * @description: 方便展示火焰图的方法、主类
 * @author: 李沛隆21081020
 * @create: 2021-10-09 09:31
 */
public class ParallelSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<Long> source = env.addSource(new ParallelSource());

        source.map((Long val) -> {
                    return val;
                })
                .timeWindowAll(Time.seconds(2))
                .sum(0)
                .print();


        env.execute("Parallel Source Demo");
    }

}

package wordcount;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: WordCountDemo
 * @description:
 * @author: 李沛隆21081020
 * @create: 2022-01-07 15:46
 */
public class WC {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2).
    }
}

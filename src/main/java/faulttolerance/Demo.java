package faulttolerance;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        env.execute("fault test");
    }
}

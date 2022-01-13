package kafka;

import kafka.bean.ClmLog;
import kafka.function.ConvertToResultMapFunc;
import kafka.function.EventFilterFunc;
import kafka.function.JsonStrToBeanMapFunc;
import kafka.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 10:57
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);

        Configuration conf = new Configuration();

        FlinkKafkaConsumer<String> myConsumer = KafkaUtils.createKafkaConsumer(conf);
        //将kafka和zookeeper配置信息加载到Flink的执行环境当中StreamExecutionEnvironment


        //添加数据源，此处选用数据流的方式，将KafKa中的数据转换成Flink的DataStream类型
        DataStream<String> stream = env.addSource(myConsumer).setParallelism(conf.getInt("FLINK.SOURCE.PARALLELISM"));

        DataStream<String> grantUseNum = stream
                .map(new JsonStrToBeanMapFunc())
                .filter(new EventFilterFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ClmLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(context -> new TimestampExtractor())
                                .withIdleness(Duration.ofMinutes(1))
                )
                .keyBy(log -> log.getKeyGroup())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .sum("grantUseNum")
                .map(new ConvertToResultMapFunc(conf.get("FLINK.RUNTIME.ENV")));

        grantUseNum.print();

        //kafka source 2
        FlinkKafkaConsumer<String> anotherConsumer = KafkaUtils.createKafkaConsumer(conf);


        //添加数据源，此处选用数据流的方式，将KafKa中的数据转换成Flink的DataStream类型
        DataStream<String> anotherStream = env.addSource(anotherConsumer);
        anotherStream.join(grantUseNum);

        //打印输出
        anotherStream.print();
        //执行Job，Flink执行环境必须要有job的执行步骤，而以上的整个过程就是一个Job
        env.execute("WordCount From KafKa data");
    }


}

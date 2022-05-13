package window;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * <p>写着玩的wordcount</p>
 *
 * @program: WordCountDemo
 * @author: 李沛隆21081020
 * @create: 2022-05-13 10:45
 */
public class WC {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\tmp\\text.txt");
        DataStream<Tuple2<String, Integer>> res = dataStreamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String s : value.split("\\s+")) {
                            out.collect(new Tuple2<>(s, 1));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.milliseconds(10))
                .sum(1);
        res.print();
        env.execute("Time window job");
    }

//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class Order {
//        private String id;
//        private Integer userId;
//        private Integer money;
//        private Long createTime;
//    }
//
//    public static class MyOrderSource extends RichParallelSourceFunction<Order> {
//        private Boolean flag = true;
//
//        @Override
//        public void run(SourceContext<Order> ctx) throws Exception {
//            Random random = new Random();
//            while (flag) {
//                Thread.sleep(1000);
//                String id = UUID.randomUUID().toString();
//                int userId = random.nextInt(3);
//                int money = random.nextInt(101);
//                long createTime = System.currentTimeMillis();
//                ctx.collect(new Order(id, userId, money, createTime));
//            }
//        }
//
//        //取消任务/执行cancle命令的时候执行
//        @Override
//        public void cancel() {
//            flag = false;
//        }
//    }
}

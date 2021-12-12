package flamegraph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redundenttm.RedundentTMDemo;
import wordcount.WordCountData;

/**
 * @program: WordCountDemo
 * @description: 测试火焰图
 * @author: 李沛隆21081020
 * @create: 2021-10-21 15:54
 */
public class FlameGraphTest {

    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(RedundentTMDemo.class);
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                        // normalize and split the line
                                        String[] tokens = value.toLowerCase().split("\\W+");

                                        // emit the pairs
                                        for (String token : tokens) {
                                            if (token.length() > 0) {
                                                out.collect(new Tuple2<>(token, 1));
                                            }
                                        }
                                        Thread.sleep(10000);
                                    }
                                })
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        // emit result
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            log.info("Printing result to stdout. Use --output to specify output path.");
            counts.print();
            counts.addSink(new PrintSinkFunction<>());
        }

        // execute program
        env.execute("Streaming WordCount");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


}

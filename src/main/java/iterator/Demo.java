package iterator;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

/**
 * @Author: lipeilong
 * @Date: 2021/8/24 10:42
 */
public class Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Integer> someIntegers = env.fromElements(-1, 2, 3, 4, 5);

        IterativeStream<Integer> iteration = someIntegers.iterate();

        DataStream<Integer> minusOne = iteration.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value - 1;
            }
        });

        DataStream<Integer> stillGreaterThanZero = minusOne.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Integer> lessThanZero = minusOne.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return (value <= 0);
            }
        });

        Iterator<Integer> collect = DataStreamUtils.collect(lessThanZero);
        lessThanZero.print();
        env.execute("Iterator Test");
    }
}


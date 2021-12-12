package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: lipeilong
 * @Date: 2021/8/26 11:35
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = env.fromElements(
                "Flink flink flink ",
                "spark spark spark",
                "Spark Spark Spark");

        AggregateOperator<Tuple2<String, Integer>> sum = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        sum.print();


        //env.execute("Flink Batch Java API Skeleton");

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         *  env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         *  .filter()
         *  .flatMap()
         *  .join()
         *  .coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * https://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */

        // execute program

    }


    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] split = s.toLowerCase().split("\\W+");

            for (String tokoen : split) {
                collector.collect(new Tuple2<String, Integer>(tokoen, 1));
            }
        }

    }

}

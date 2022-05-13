package state.mapstate;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

/**
 * <p></p>
 *
 * @program: WordCountDemo
 * @author: 李沛隆21081020
 * @create: 2022-05-11 16:10
 */
public class CountAverageWithMapState
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private MapState<String, Long> mapState;

    /***状态初始化*/
    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor descriptor = new MapStateDescriptor("MapDescriptor", String.class, String.class);
        mapState = getRuntimeContext().getMapState(descriptor);

    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {

        //获取状态
        mapState.put(UUID.randomUUID().toString(), element.f1);
        List<Long> allEles = Lists.newArrayList(mapState.values());

        if (allEles.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Long ele : allEles) {
                count++;
                sum += ele;
            }
            double avg = (double) sum / count;
            collector.collect(Tuple2.of(element.f0, avg));
            mapState.clear();
        }
    }


}

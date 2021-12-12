package testmanagedstate;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: lipeilong
 * @Date: 2021/8/25 14:49
 */
public class TestKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple3<String, String, String>, Tuple2<String, Long>> implements CheckpointedFunction {

    //使用slf4j适配日志
    private org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TestProcessFunction1.class);
    //用于存储业务State
    private MapStateDescriptor<String, String> testMapStateDescriptor;
    private MapState<String, String> testMapState;
    //TTL Timer State
    private MapStateDescriptor<String, Long> testTimerMapStateDescriptor;
    private MapState<String, Long> testTimerMapState;
    private int testMapStateTTL;

    public TestKeyedProcessFunction(int testMapStateTTL) {
        this.testMapStateTTL = testMapStateTTL;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //初始化用于存储业务State
        testMapStateDescriptor =
                new MapStateDescriptor<String, String>("test", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        testMapState =
                context.getKeyedStateStore().getMapState(testMapStateDescriptor);
        //初始化TTL Timer State
        testTimerMapStateDescriptor =
                new MapStateDescriptor<String, Long>("testTimer", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
        testTimerMapState =
                context.getKeyedStateStore().getMapState(testTimerMapStateDescriptor);
    }

    @Override
    public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        //check是否需要给该key注册TTL Timer
        String key = value.f0;
        if (!testTimerMapState.contains(key)) {
            long ttlTimer = testMapStateTTL + System.currentTimeMillis();
            ctx.timerService().registerProcessingTimeTimer(ttlTimer);
            testTimerMapState.put(key, ttlTimer);
        }
        /*
            . . .
            业务处理逻辑（业务异常需要捕获，运行时异常根据业务需要捕获）
            业务缓存放到testMapState对象中
            传输数据不能null
            . . .
         */
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        String key = ctx.getCurrentKey().getField(0);
        long timer = testTimerMapState.get(key);
        //确定TTL Timer时间到了开始清理State，包括TTL Timer State
        if (timer == timestamp) {
            testMapState.clear();
            testTimerMapState.clear();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            //关闭资源（很重要）
            //如db、redis、hbase、工具类、线程池等资源
        }catch (Exception e) {
            LOG.error("errorMsg", e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }
}

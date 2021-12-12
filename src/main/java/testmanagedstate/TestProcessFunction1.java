package testmanagedstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: lipeilong
 * @Date: 2021/8/25 10:53
 */
public class TestProcessFunction1 extends ProcessFunction<String, Long> implements CheckpointedFunction {

    //使用slf4j适配日志
    private org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TestProcessFunction1.class);

    //Operate State里面使用Map类型数据结构
    private ListStateDescriptor<Map<String, String>> testListStateDescriptor;
    private ListState<Map<String, String>> testListState;
    private Map<String, String> testMap;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        //初始化状态描述器，此处使用ListState的状态生成
        testListStateDescriptor =
                new ListStateDescriptor<>("test", TypeInformation.of(new TypeHint<Map<String, String>>() {
                }));
        //初始化listState，此处使用入参context初始化
        testListState =
                context.getOperatorStateStore().getListState(testListStateDescriptor);
        //从ListState中初始化Map
        if (testListState != null) {
            Iterable<Map<String, String>> testListStateIterable = testListState.get();
            if (testListStateIterable.iterator().hasNext()) {
                testMap = testListStateIterable.iterator().next();
            }
        }
        if (testMap == null) {
            testMap = new HashMap<>();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            //打开资源（很重要）
            //如db、redis、hbase、工具类、线程池等资源
        } catch (Exception e) {
            LOG.error("errorMsg", e);
            throw e;
        }
    }

    public void processElement(String value, Context ctx, Collector<Long> out) {
        /*
            . . .
            业务处理逻辑（业务异常需要捕获，运行时异常根据业务需要捕获）
            业务缓存放到testMap对象中
            传输数据不能null
            . . .
         */

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //确定Map已经放入ListState
        if (!testListState.get().iterator().hasNext()) {
            testListState.add(testMap);
        }
    }
}

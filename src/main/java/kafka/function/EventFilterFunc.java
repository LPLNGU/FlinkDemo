package kafka.function;

import kafka.bean.ClmLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.logging.Filter;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 17:34
 */
public class EventFilterFunc implements FilterFunction<ClmLog> {
    private static final String VALID_EVENT_NAME = "CmfAccountOrderEvent";
    private static final List<String> VALID_IDENTIFIER_LIST =
            Lists.newArrayList("TA04010330", "TA04011010");

    @Override
    public boolean filter(ClmLog clmLog) throws Exception {
        if (clmLog == null) {
            return false;
        }

        return clmLog.getEvent().equals(VALID_EVENT_NAME) &&
                VALID_IDENTIFIER_LIST
                        .stream()
                        .anyMatch(i -> i.equals(clmLog.getInteridentifier()));
    }
}

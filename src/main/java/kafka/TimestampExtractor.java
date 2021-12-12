package kafka;

import kafka.bean.ClmLog;
import kafka.util.TimeUtils;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Author: lipeilong
 * @Date: 2021/9/2 19:43
 */
public class TimestampExtractor implements TimestampAssigner<ClmLog> {

    public static final Logger LOG = LoggerFactory.getLogger(TimestampExtractor.class);


    @Override
    public long extractTimestamp(ClmLog clmLog,long l) {
        String str = clmLog.getInterceptTime();
        try {
            return TimeUtils.strInMsToTimestamp(str);
        } catch (Exception e) {
            LOG.warn("Error when parsing intercept time", e);
            return System.currentTimeMillis();
        }
    }
}

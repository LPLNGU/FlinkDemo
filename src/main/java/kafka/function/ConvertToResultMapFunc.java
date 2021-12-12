package kafka.function;

import kafka.bean.ClmLog;
import kafka.bean.OutputRecord;
import kafka.util.JsonUtils;
import kafka.util.TimeUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.math.BigDecimal;
import java.text.ParseException;

import static java.math.BigDecimal.ROUND_HALF_UP;

public class ConvertToResultMapFunc implements MapFunction<ClmLog, String> {
    private String env;

    public ConvertToResultMapFunc(String env) {
        this.env = env;
    }

    @Override
    public String map(ClmLog log) throws Exception {
        String logType = ("add".equals(log.getGrantUseSign())) ?
                "memberOrderTypeAdd" : "memberOrderTypeUse";
        String logTime = getWindowStart(log.getInterceptTime());
        String memberNum = "cs" + log.getMemberNumber();
        BigDecimal drill = BigDecimal.valueOf(log.getGrantUseNum())
                .setScale(2, ROUND_HALF_UP);

        OutputRecord result = OutputRecord.Builder
                .aOutputRecord()
                .withLogType(logType)
                .withLogTime(logTime)
                .withMemberNumber(memberNum)
                .withOrderType(log.getOrderType())
                .withOrderTypeDesc(log.getOrderTypeDesc())
                .withDrill(drill)
                .build();

        String outputJson = JsonUtils.toJson(result);
        if ("sit".equalsIgnoreCase(env)) {
            System.out.println(outputJson);
        }

        return outputJson;
    }

    private String getWindowStart(String logTime) {
        long tsInMs;
        try {
            tsInMs = TimeUtils.strInMsToTimestamp(logTime);
        } catch (ParseException e) {
            tsInMs = System.currentTimeMillis();
        }

        long startInMs = TimeWindow.getWindowStartWithOffset(tsInMs,
            Time.hours(-8).toMilliseconds(),
            Time.days(1).toMilliseconds());
        return TimeUtils.timestampToStr(startInMs);
    }
}

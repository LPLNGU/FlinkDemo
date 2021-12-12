package kafka.function;


import kafka.bean.ClmLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 17:06
 */
public class JsonStrToBeanMapFunc implements MapFunction<String, ClmLog> {

    private static final Logger LOG =LoggerFactory.getLogger(JsonStrToBeanMapFunc.class);


    @Override
    public ClmLog map(String value) throws Exception {


        return null;
    }
}

package sqludf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @program: WordCountDemo
 * @description:
 * @author: 李沛隆21081020
 * @create: 2022-01-27 09:38
 */
public class myUdf extends ScalarFunction {
    private Long current = 0L;
    private static final Logger LOGGER = LoggerFactory.getLogger(myUdf.class);
    public Long eval(Long a) throws Exception {
        if(current == 0L) {
            current = a;
        } else  {
            current += 1;
        }
        LOGGER.error("The current is : " + current );
        return current;
    }
}

package kafka.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: lipeilong
 * @Date: 2021/9/2 9:53
 */
public class TimeUtils {
    private static SimpleDateFormat sdfInMS
            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static SimpleDateFormat sdfInSec
            = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static long strInMsToTimestamp(String str)
            throws ParseException {
        return sdfInMS.parse(str).getTime();
    }

    public static String timestampToStr(long timestamp) {
        return sdfInSec.format(new Date(timestamp));
    }
}

package kafka.util;

import com.google.gson.Gson;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Author: lipeilong
 * @Date: 2021/9/1 17:16
 */
public class JsonUtils {
    public static final Logger LOG =
            LoggerFactory.getLogger(JsonUtils.class);

    private static Gson gson = new Gson();

    public static <T> T fromJson(String str, Class<T> cls) {
        if (StringUtils.isNullOrWhitespaceOnly(str)) {
            return null;
        }
        return gson.fromJson(str, cls);
    }

    public static String toJson(Object o) {
        if (o == null) {
            return null;
        }
        return gson.toJson(o);
    }
}

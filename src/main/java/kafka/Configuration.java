package kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private Properties properties = new Properties();

    public Configuration() {
        try {
            InputStream inputStream = Configuration.class
                .getResourceAsStream("/conf.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

    public int getInt(String key, int defaultVal) {
        String value = properties.getProperty(key);
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return defaultVal;
        }
    }

    public int getInt(String key) {
        String value = properties.getProperty(key);
        return Integer.parseInt(value);
    }
}

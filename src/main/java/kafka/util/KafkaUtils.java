package kafka.util;

import kafka.Configuration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 15:55
 */
public class KafkaUtils {
    public static FlinkKafkaConsumer<String> createKafkaConsumer(
            Configuration conf) {
        Properties props = new Properties();
        props.put("bootstrap.servers",
                conf.get("KAFKA.SOURCE.BOOTSTRAP.SERVERS"));
        props.put("zookeeper.connect",
                conf.get("KAFKA.SOURCE.ZOOKEEPER.CONNECT"));
        props.put("group.id", conf.get("KAFKA.SOURCE.GROUP.ID"));

        FlinkKafkaConsumer<String> kafkaConsumer08 =
                new FlinkKafkaConsumer<>(conf.get("KAFKA.SOURCE.TOPIC"),
                        new SimpleStringSchema(), props);
        kafkaConsumer08.setStartFromLatest();
        kafkaConsumer08.setCommitOffsetsOnCheckpoints(true);

        return kafkaConsumer08;
    }

    public static FlinkKafkaConsumer<String> createKafkaSink(
            Configuration conf) {
        Properties props = new Properties();
        props.put("bootstrap.servers",
                conf.get("KAFKA.SINK.BOOTSTRAP.SERVERS"));
        props.put("zookeeper.connect",
                conf.get("KAFKA.SINK.ZOOKEEPER.CONNECT"));

        return new FlinkKafkaConsumer<>(conf.get("KAFKA.SINK.TOPIC"),
                new SimpleStringSchema(), props);
    }
}

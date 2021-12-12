package kafka.upserttest;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.junit.jupiter.api.Test;

import java.util.Properties;


/**
 * @program: WordCountDemo
 * @description: Upsert kafka流的Demo 可以本地运行
 * @author: 李沛隆21081020
 * @create: 2021-10-13 09:34
 */
public class KafkaUpsertTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000); // 10s 做一个checkpoint

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 每次Query时动态指定或覆写表的option,
        // 需要加上配置set table.dynamic-table-options.enabled=true来开启，默认关闭
        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);

        // 创建kafka source
        KafakSource(tEnv);
        // 创建upsert kafka sink
        UpsertKafakSink(tEnv);

        System.out.println("---> 3. 测试  .... ");
        // 创建临时表
        String view_total_pvuv_min = "CREATE VIEW view_total_pvuv_min AS\n" +
                "SELECT\n" +
                "     dt AS do_date,                    -- 时间分区\n" +
//                "     client_ip,                        -- 客户端的IP\n" +
                "     count (client_ip) AS pv,          -- 客户端的IP\n" +
                "     count (DISTINCT client_ip) AS uv, -- 客户端去重\n" +
                "     max(access_time) AS access_time   -- 请求的时间\n" +
                "FROM\n" +
                "    source_ods_fact_user_ippv\n" +
                "GROUP BY dt";
        tEnv.executeSql(view_total_pvuv_min);

        // 写入数据
        String result_total_pvuv_min = "INSERT INTO result_total_pvuv_min\n" +
                "SELECT\n" +
                "      do_date,    --  时间分区\n" +
                "      cast(DATE_FORMAT (access_time,'HH:mm') AS STRING) AS do_min,-- 分钟级别的时间\n" +
//                "      client_ip,\n" +
                "      pv,\n" +
                "      uv,\n" +
                "      CURRENT_TIMESTAMP AS currenttime -- 当前时间\n" +
                "FROM\n" +
                "      view_total_pvuv_min";
        tEnv.executeSql(result_total_pvuv_min);

        // 从upsert kafka 中查询数据
        String select_data = "select * from result_total_pvuv_min";
        TableResult result = tEnv.executeSql(select_data);
        result.print();

    }

    /**
     * Kafka 数据源
     * 用户的ip pv信息，一个用户在一天内可以有很多次pv
     *
     * @param tEnv
     */
    public static void KafakSource(StreamTableEnvironment tEnv) {
        System.out.println("---> 1. create kafka source table  .... ");
        String kafakSource = "CREATE TABLE source_ods_fact_user_ippv (\n" +
                "    user_id      STRING,       -- 用户ID\n" +
                "    client_ip    STRING,       -- 客户端IP\n" +
                "    client_info  STRING,       -- 设备机型信息\n" +
                "    pagecode     STRING,       -- 页面代码\n" +
                "    access_time  TIMESTAMP,    -- 请求时间\n" +
                "    dt           STRING,       -- 时间分区天\n" +
                "    WATERMARK FOR access_time AS access_time - INTERVAL '5' SECOND  -- 定义watermark\n" +
                ") WITH (\n" +
                "   'connector' = 'kafka', -- 使用 kafka connector\n" +
                "    'topic' = 'hsj', -- kafka主题\n" +
                "    'scan.startup.mode' = 'latest-offset', -- 偏移量\n" +
                "    'properties.group.id' = 'hsj123', -- 消费者组\n" +
                "    'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092',\n" +
                "    'format' = 'json', -- 数据源格式为json\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";
        tEnv.executeSql(kafakSource);
    }

    /**
     * 普通 kafka sink
     * 统计每分钟的PV、UV，并将结果存储在Kafka中
     *
     * @param tEnv
     */
    public static void KafakSink(StreamTableEnvironment tEnv) {
        System.out.println("---> 2. create upsert kafka sink table  .... ");
        String upsertKafakSink = "CREATE TABLE result_total_pvuv_min (\n" +
                "    do_date     STRING,     -- 统计日期\n" +
                "    do_min      STRING,     -- 统计分钟\n" +
//                "    client_ip   STRING,     -- 客户端IP\n" +
                "    pv          BIGINT,     -- 点击量\n" +
                "    uv          BIGINT,     -- 一天内同个访客多次访问仅计算一个UV\n" +
                "    currenttime TIMESTAMP  -- 当前时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'hsj-sink1',\n" +
                "  'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092',\n" +
                "  'format' = 'json'\n" +
                ")";
        tEnv.executeSql(upsertKafakSink);
    }

    /**
     * upsert kafka sink
     * 统计每分钟的PV、UV，并将结果存储在Kafka中
     *
     * @param tEnv
     */
    public static void UpsertKafakSink(StreamTableEnvironment tEnv) {
        System.out.println("---> 2. create upsert kafka sink table  .... ");
        String upsertKafakSink = "CREATE TABLE result_total_pvuv_min (\n" +
                "    do_date     STRING,     -- 统计日期\n" +
                "    do_min      STRING,     -- 统计分钟\n" +
//                "    client_ip   STRING,     -- 客户端IP\n" +
                "    pv          BIGINT,     -- 点击量\n" +
                "    uv          BIGINT,     -- 一天内同个访客多次访问仅计算一个UV\n" +
                "    currenttime TIMESTAMP,  -- 当前时间\n" +
                "    PRIMARY KEY (do_date, do_min) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'hsj-sink1',\n" +
                "  'properties.bootstrap.servers' = 'kafkasitoltp01broker01.cnsuning.com:9092,kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092',\n" +
                "  'key.json.ignore-parse-errors' = 'true',\n" +
                "  'value.json.fail-on-missing-field' = 'false',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'value.fields-include' = 'EXCEPT_KEY' -- key不出现kafka消息的value中\n" +
                ")";
        tEnv.executeSql(upsertKafakSink);
    }

    private static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, Long>> {
        private TableSchema schema;
        private String[] keyFields;
        private boolean isAppendOnly;

        private String[] fieldNames;
        private TypeInformation<?>[] fieldTypes;

        public MemoryUpsertSink() {

        }

        public MemoryUpsertSink(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public void setKeyFields(String[] keys) {
            this.keyFields = keys;
        }

        @Override
        public void setIsAppendOnly(Boolean isAppendOnly) {
            this.isAppendOnly = isAppendOnly;
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getRecordType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
            });
        }


        @Override
        public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
            return dataStream.addSink(new DataSink()).setParallelism(1);
        }

        @Override
        public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink();
            memoryUpsertSink.setFieldNames(fieldNames);
            memoryUpsertSink.setFieldTypes(fieldTypes);
            memoryUpsertSink.setKeyFields(keyFields);
            memoryUpsertSink.setIsAppendOnly(isAppendOnly);

            return memoryUpsertSink;
        }

        @Override
        public String[] getFieldNames() {
            return schema.getFieldNames();
        }

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return schema.getFieldTypes();
        }

        public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
            this.fieldTypes = fieldTypes;
        }
    }

    private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>> {
        public DataSink() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {
            System.out.println("send message:" + value);
        }
    }

    /**
     * 测试从upsert kafka 中读取数据
     */
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafkasitoltp01broker01.cnsuning.com:9092," +
                "kafkasitoltp01broker02.cnsuning.com:9092,kafkasitoltp01broker03.cnsuning.com:9092");
        properties.setProperty("group.id", "BD_hsj");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("hsj-sink1", new SimpleStringSchema(), properties));

        dataStream.print();

        env.execute();
    }
}

package hive

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object ScalaHiveTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(2)
    env.enableCheckpointing(5000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    tEnv.getConfig.getConfiguration.setBoolean("table.dynamic-table-options.enabled", true)
    //datastream
    /*val properties = new Properties()
    properties.put("bootstrap.servers", "kafkasitoltp01broker01.cnsuning.com:9092," +
      "kafkasitoltp01broker03.cnsuning.com:9092," +
      "kafkasitoltp01broker03.cnsuning.com:9092")
    properties.put("group.id", "BD_hsj-sink1")
    val topicName = "hsj-sink1"
    val consumer = new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()
    val streamSource = env.addSource(consumer)
    streamSource.print()*/

    /*val fileSystemSink = StreamingFileSink
      .forRowFormat(new Path("D:\\tmp"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toSeconds(15))
          .withInactivityInterval(TimeUnit.SECONDS.toSeconds(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build()) //设置文件滚动条件
      .build()
    streamSource.addSink(fileSystemSink)*/

    //sql
    tEnv.executeSql(
      "CREATE TABLE kafka_source (\n" +
        "    syscode STRING\n" +
        ") WITH (\n" +
        " 'connector' = 'kafka',\n" +
        " 'topic' = 'hsj-sink1',\n" +
        " 'properties.bootstrap.servers' = " +
        "'kafkasitoltp01broker01.cnsuning.com:9092," +
        "kafkasitoltp01broker02.cnsuning.com:9092," +
        "kafkasitoltp01broker03.cnsuning.com:9092'," +
        " 'format' = 'json',\n" +
        " 'scan.startup.mode' = 'latest-offset')"
    )
    tEnv.executeSql(
      "CREATE TABLE fs_table_tmall_gds (\n" +
        "    syscode STRING)" +
        " PARTITIONED BY (syscode) WITH (\n" +
        "    'connector' = 'filesystem',\n" +
        "    'path' = 'hdfs://RouterSit/user/bigdata/lpl',\n" +
        //        "    'path' = 'D:\\tmp',\n" +
        "    'format' = 'json',\n" +
        "    'sink.rolling-policy.file-size' = '10MB',\n" +
        "    'sink.rolling-policy.rollover-interval' = '15sec')"
    )
    tEnv.executeSql(
      "INSERT INTO\n" +
        "    fs_table_tmall_gds\n" +
        "SELECT\n" +
        "    syscode\n" +
        "FROM\n" +
        "    kafka_source /*+ OPTIONS('properties.group.id'='BD_hsj-sink1') */"
    )
    //    env.execute()
  }

}

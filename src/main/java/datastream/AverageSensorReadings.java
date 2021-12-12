package datastream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 10:16
 */
public class AverageSensorReadings {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<SensorReading> sensorData ;

    }
}

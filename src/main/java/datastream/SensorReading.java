package datastream;

import java.util.Objects;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 9:42
 */
public class SensorReading {

    private String id;
    private Long TimeStamp;
    private Double Temperature;

    public SensorReading(String id, Long timeStamp, Double temperature) {
        this.id = id;
        TimeStamp = timeStamp;
        Temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", TimeStamp=" + TimeStamp +
                ", Temperature=" + Temperature +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorReading that = (SensorReading) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(TimeStamp, that.TimeStamp) &&
                Objects.equals(Temperature, that.Temperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, TimeStamp, Temperature);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimeStamp() {

        return TimeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        TimeStamp = timeStamp;
    }

    public Double getTemperature() {
        return Temperature;
    }

    public void setTemperature(Double temperature) {
        Temperature = temperature;
    }
}

package kafka.bean;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

public class OutputRecord implements Serializable {
    private String logType;
    private String logTime;
    private String memberNumber;
    private String orderType;
    private String orderTypeDesc;
    private BigDecimal drill;

    public OutputRecord() {
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public String getMemberNumber() {
        return memberNumber;
    }

    public void setMemberNumber(String memberNumber) {
        this.memberNumber = memberNumber;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public String getOrderTypeDesc() {
        return orderTypeDesc;
    }

    public void setOrderTypeDesc(String orderTypeDesc) {
        this.orderTypeDesc = orderTypeDesc;
    }

    public BigDecimal getDrill() {
        return drill;
    }

    public void setDrill(BigDecimal drill) {
        this.drill = drill;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputRecord that = (OutputRecord) o;
        return Objects.equals(logType, that.logType) &&
                Objects.equals(logTime, that.logTime) &&
                Objects.equals(memberNumber, that.memberNumber) &&
                Objects.equals(orderType, that.orderType) &&
                Objects.equals(orderTypeDesc, that.orderTypeDesc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logType, logTime, memberNumber, orderType, orderTypeDesc);
    }

    @Override
    public String toString() {
        return "StatisticResult{" +
                "logType='" + logType + '\'' +
                ", logTime='" + logTime + '\'' +
                ", memberNumber='" + memberNumber + '\'' +
                ", orderType='" + orderType + '\'' +
                ", orderTypeDesc='" + orderTypeDesc + '\'' +
                ", drill=" + drill +
                '}';
    }

    public static final class Builder {
        private OutputRecord outputRecord;

        private Builder() {
            outputRecord = new OutputRecord();
        }

        public static Builder aOutputRecord() {
            return new Builder();
        }

        public Builder withLogType(String logType) {
            outputRecord.setLogType(logType);
            return this;
        }

        public Builder withLogTime(String logTime) {
            outputRecord.setLogTime(logTime);
            return this;
        }

        public Builder withMemberNumber(String memberNumber) {
            outputRecord.setMemberNumber(memberNumber);
            return this;
        }

        public Builder withOrderType(String orderType) {
            outputRecord.setOrderType(orderType);
            return this;
        }

        public Builder withOrderTypeDesc(String orderTypeDesc) {
            outputRecord.setOrderTypeDesc(orderTypeDesc);
            return this;
        }

        public Builder withDrill(BigDecimal drill) {
            outputRecord.setDrill(drill);
            return this;
        }

        public OutputRecord build() {
            return outputRecord;
        }
    }
}

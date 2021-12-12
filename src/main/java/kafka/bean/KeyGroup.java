package kafka.bean;


import org.apache.flink.api.java.functions.KeySelector;

import java.util.Objects;


/**
 * @Author: lipeilong
 * @Date: 2021/9/1 19:11
 */
public class KeyGroup implements KeySelector<ClmLog, KeyGroup> {
    private String memberNumber;
    private String orderType;
    private String orderTypeDesc;
    private String grantUseSign;

    public KeyGroup() {
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

    public String getGrantUseSign() {
        return grantUseSign;
    }

    public void setGrantUseSign(String grantUseSign) {
        this.grantUseSign = grantUseSign;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyGroup keyGroup = (KeyGroup) o;
        return Objects.equals(memberNumber, keyGroup.memberNumber) &&
                Objects.equals(orderType, keyGroup.orderType) &&
                Objects.equals(orderTypeDesc, keyGroup.orderTypeDesc) &&
                Objects.equals(grantUseSign, keyGroup.grantUseSign);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberNumber, orderType, orderTypeDesc, grantUseSign);
    }

    @Override
    public String toString() {
        return "KeyGroup{" +
                "memberNumber='" + memberNumber + '\'' +
                ", orderType='" + orderType + '\'' +
                ", orderTypeDesc='" + orderTypeDesc + '\'' +
                ", grantUseSign='" + grantUseSign + '\'' +
                '}';
    }

    @Override
    public KeyGroup getKey(ClmLog value) throws Exception {
        return KeyGroupBuilder.aKeyGroup()
                .withGrantUseSign(value.getGrantUseSign())
                .withMemberNumber(value.getMemberNumber())
                .withOrderType(value.getOrderType())
                .withOrderTypeDesc(value.getOrderTypeDesc())
                .build();
    }

    public static final class KeyGroupBuilder {
        private String memberNumber;
        private String orderType;
        private String orderTypeDesc;
        private String grantUseSign;

        private KeyGroupBuilder() {
        }

        public static KeyGroupBuilder aKeyGroup() {
            return new KeyGroupBuilder();
        }

        public KeyGroupBuilder withMemberNumber(String memberNumber) {
            this.memberNumber = memberNumber;
            return this;
        }

        public KeyGroupBuilder withOrderType(String orderType) {
            this.orderType = orderType;
            return this;
        }

        public KeyGroupBuilder withOrderTypeDesc(String orderTypeDesc) {
            this.orderTypeDesc = orderTypeDesc;
            return this;
        }

        public KeyGroupBuilder withGrantUseSign(String grantUseSign) {
            this.grantUseSign = grantUseSign;
            return this;
        }

        public KeyGroup build() {
            KeyGroup keyGroup = new KeyGroup();
            keyGroup.orderTypeDesc = this.orderTypeDesc;
            keyGroup.grantUseSign = this.grantUseSign;
            keyGroup.memberNumber = this.memberNumber;
            keyGroup.orderType = this.orderType;
            return keyGroup;
        }
    }
}

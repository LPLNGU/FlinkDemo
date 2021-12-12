package kafka.bean;

import java.io.Serializable;

/**
 * @Author: lipeilong
 * @Date: 2021/9/1 17:12
 */
public class ClmLog implements Serializable {
    private String event;
    private String memberNumber;
    private String interceptTime;
    private String accountType;
    private String grantUseSign;
    private String orderItemId;
    private double grantUseNum;
    private String orderType;
    private String orderTypeDesc;
    private String interidentifier;

    public ClmLog() {
    }

    public KeyGroup getKeyGroup() {
        return KeyGroup.KeyGroupBuilder.aKeyGroup()
                .withMemberNumber(memberNumber)
                .withOrderType(orderType)
                .withOrderTypeDesc(orderTypeDesc)
                .withGrantUseSign(grantUseSign)
                .build();
    }

    @Override
    public String toString() {
        return "ClmLog{" +
                "event='" + event + '\'' +
                ", memberNumber='" + memberNumber + '\'' +
                ", interceptTime='" + interceptTime + '\'' +
                ", accountType='" + accountType + '\'' +
                ", grantUseSign='" + grantUseSign + '\'' +
                ", orderItemId='" + orderItemId + '\'' +
                ", grantUseNum=" + grantUseNum +
                ", orderType='" + orderType + '\'' +
                ", orderTypeDesc='" + orderTypeDesc + '\'' +
                ", interidentifier='" + interidentifier + '\'' +
                '}';
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getMemberNumber() {
        return memberNumber;
    }

    public void setMemberNumber(String memberNumber) {
        this.memberNumber = memberNumber;
    }

    public String getInterceptTime() {
        return interceptTime;
    }

    public void setInterceptTime(String interceptTime) {
        this.interceptTime = interceptTime;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public String getGrantUseSign() {
        return grantUseSign;
    }

    public void setGrantUseSign(String grantUseSign) {
        this.grantUseSign = grantUseSign;
    }

    public String getOrderItemId() {
        return orderItemId;
    }

    public void setOrderItemId(String orderItemId) {
        this.orderItemId = orderItemId;
    }

    public double getGrantUseNum() {
        return grantUseNum;
    }

    public void setGrantUseNum(double grantUseNum) {
        this.grantUseNum = grantUseNum;
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

    public String getInteridentifier() {
        return interidentifier;
    }

    public void setInteridentifier(String interidentifier) {
        this.interidentifier = interidentifier;
    }
}

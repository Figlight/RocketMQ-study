package com.example.rocketmqstudy.springboot.model;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * 用于演示 JSON 对象消息和订单状态顺序消息的模型。
 */
public class OrderMessage {

    /** 订单号，也是顺序消息的队列选择键。 */
    @NotBlank(message = "订单号不能为空")
    private String orderNumber;

    /** 用户编号。 */
    @NotBlank(message = "用户编号不能为空")
    private String userId;

    /** 订单当前状态。 */
    @NotBlank(message = "订单状态不能为空")
    private String status;

    /** 同一订单内的业务步骤序号。 */
    @Min(value = 1, message = "步骤序号必须大于 0")
    private int sequence;

    /** 创建空订单消息，供 JSON 反序列化使用。 */
    public OrderMessage() {
    }

    /**
     * 创建订单消息。
     *
     * @param orderNumber 订单号。
     * @param userId 用户编号。
     * @param status 订单状态。
     * @param sequence 业务步骤序号。
     */
    public OrderMessage(String orderNumber, String userId, String status, int sequence) {
        this.orderNumber = orderNumber;
        this.userId = userId;
        this.status = status;
        this.sequence = sequence;
    }

    /** @return 订单号。 */
    public String getOrderNumber() {
        return orderNumber;
    }

    /** @param orderNumber 订单号。 */
    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    /** @return 用户编号。 */
    public String getUserId() {
        return userId;
    }

    /** @param userId 用户编号。 */
    public void setUserId(String userId) {
        this.userId = userId;
    }

    /** @return 订单状态。 */
    public String getStatus() {
        return status;
    }

    /** @param status 订单状态。 */
    public void setStatus(String status) {
        this.status = status;
    }

    /** @return 业务步骤序号。 */
    public int getSequence() {
        return sequence;
    }

    /** @param sequence 业务步骤序号。 */
    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    /**
     * 输出便于观察消费结果的订单内容。
     *
     * @return 订单消息文本。
     */
    @Override
    public String toString() {
        return "OrderMessage{" +
                "orderNumber='" + orderNumber + '\'' +
                ", userId='" + userId + '\'' +
                ", status='" + status + '\'' +
                ", sequence=" + sequence +
                '}';
    }
}

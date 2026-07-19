package com.example.rocketmqstudy.messagemode.model;

/**
 * 订单流程事件，用于演示相同订单号的消息被路由到同一个队列。
 */
public final class OrderEvent {

    /**
     * 事件序号，用于区分同一订单下的多个步骤。
     */
    private final int eventId;

    /**
     * 订单号，也是顺序消息的分区业务键。
     */
    private final long orderNumber;

    /**
     * 当前订单步骤说明。
     */
    private final String description;

    /**
     * 创建订单流程事件。
     *
     * @param eventId 事件序号。
     * @param orderNumber 订单号。
     * @param description 订单步骤说明。
     */
    public OrderEvent(int eventId, long orderNumber, String description) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.description = description;
    }

    /**
     * 获取事件序号。
     *
     * @return 事件序号。
     */
    public int getEventId() {
        return eventId;
    }

    /**
     * 获取订单号。
     *
     * @return 订单号。
     */
    public long getOrderNumber() {
        return orderNumber;
    }

    /**
     * 获取订单步骤说明。
     *
     * @return 订单步骤说明。
     */
    public String getDescription() {
        return description;
    }

    /**
     * 将订单事件转换为便于控制台阅读的消息正文。
     *
     * @return 订单事件文本。
     */
    public String toMessageBody() {
        return "OrderEvent{eventId=" + eventId
                + ", orderNumber=" + orderNumber
                + ", description='" + description + "'}";
    }
}

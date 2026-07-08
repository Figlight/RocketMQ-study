package com.example.rocketmqstudy.introduction.model;

/**
 * 订单创建事件，代表生产者发送到 MQ 中的一条业务消息。
 */
public final class OrderPlacedEvent {

    /**
     * 订单编号，用于唯一标识一笔订单。
     */
    private final String orderId;

    /**
     * 用户编号，用于标识这笔订单属于哪个用户。
     */
    private final String userId;

    /**
     * 订单金额，用于模拟消费者处理业务时需要读取的消息内容。
     */
    private final int amount;

    /**
     * 创建订单事件。
     *
     * @param orderId 订单编号。
     * @param userId 用户编号。
     * @param amount 订单金额。
     */
    public OrderPlacedEvent(String orderId, String userId, int amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    /**
     * 获取订单编号。
     *
     * @return 订单编号。
     */
    public String getOrderId() {
        return orderId;
    }

    /**
     * 获取用户编号。
     *
     * @return 用户编号。
     */
    public String getUserId() {
        return userId;
    }

    /**
     * 获取订单金额。
     *
     * @return 订单金额。
     */
    public int getAmount() {
        return amount;
    }
}

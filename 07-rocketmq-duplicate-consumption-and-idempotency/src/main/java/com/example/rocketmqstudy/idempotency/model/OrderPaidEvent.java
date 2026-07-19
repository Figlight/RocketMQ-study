package com.example.rocketmqstudy.idempotency.model;

/**
 * 订单支付完成事件，使用订单号构造稳定的业务唯一 Key。
 */
public final class OrderPaidEvent {

    /** 事件类型，用于避免不同业务事件仅凭订单号发生 Key 冲突。 */
    private static final String EVENT_TYPE = "ORDER_PAID";

    /** 订单号。 */
    private final String orderNumber;

    /** 用户编号。 */
    private final String userId;

    /** 本次应发放的积分。 */
    private final int rewardPoints;

    /**
     * 创建订单支付完成事件。
     *
     * @param orderNumber 订单号。
     * @param userId 用户编号。
     * @param rewardPoints 应发放积分，必须大于零。
     */
    public OrderPaidEvent(String orderNumber, String userId, int rewardPoints) {
        if (orderNumber == null || orderNumber.isBlank()) {
            throw new IllegalArgumentException("订单号不能为空");
        }
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("用户编号不能为空");
        }
        if (rewardPoints <= 0) {
            throw new IllegalArgumentException("奖励积分必须大于 0");
        }
        this.orderNumber = orderNumber;
        this.userId = userId;
        this.rewardPoints = rewardPoints;
    }

    /** @return 订单号。 */
    public String getOrderNumber() {
        return orderNumber;
    }

    /** @return 用户编号。 */
    public String getUserId() {
        return userId;
    }

    /** @return 应发放积分。 */
    public int getRewardPoints() {
        return rewardPoints;
    }

    /**
     * 返回稳定业务唯一 Key，同一订单的支付完成事件始终得到相同结果。
     *
     * @return 事件类型与订单号组成的业务 Key。
     */
    public String businessKey() {
        return EVENT_TYPE + ":" + orderNumber;
    }

    /**
     * 将事件编码为无需额外 JSON 依赖的演示消息正文。
     *
     * @return 竖线分隔的消息正文。
     */
    public String toMessageBody() {
        return orderNumber + "|" + userId + "|" + rewardPoints;
    }

    /**
     * 从消息正文解析订单支付完成事件。
     *
     * @param body 竖线分隔的消息正文。
     * @return 解析后的业务事件。
     */
    public static OrderPaidEvent fromMessageBody(String body) {
        String[] parts = body.split("\\|", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("消息正文格式应为 orderNumber|userId|rewardPoints");
        }
        return new OrderPaidEvent(parts[0], parts[1], Integer.parseInt(parts[2]));
    }
}

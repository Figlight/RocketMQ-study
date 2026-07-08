package com.example.rocketmqstudy.introduction.service;

import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;
import com.example.rocketmqstudy.introduction.mq.InMemoryMessageQueue;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * 订单系统，负责产生订单创建事件并发送给 MQ。
 */
public final class OrderSystem {

    /**
     * 内存 MQ，用于保存订单事件并转交给后台消费者。
     */
    private final InMemoryMessageQueue messageQueue;

    /**
     * 创建订单系统。
     *
     * @param messageQueue 内存 MQ 实例。
     */
    public OrderSystem(InMemoryMessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    /**
     * 下单方法，只负责生成订单事件，不直接调用库存、优惠券和积分系统。
     *
     * @param orderId 订单编号。
     * @param userId 下单用户编号。
     * @param amount 订单金额。
     */
    public void placeOrder(String orderId, String userId, int amount) {
        OrderPlacedEvent event = new OrderPlacedEvent(orderId, userId, amount);
        messageQueue.publish(event);
        log("订单系统完成下单主流程：" + orderId + "，后续业务交给 MQ 异步处理");
    }
}

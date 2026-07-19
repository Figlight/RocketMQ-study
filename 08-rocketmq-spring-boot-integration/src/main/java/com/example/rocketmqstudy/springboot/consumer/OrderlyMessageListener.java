package com.example.rocketmqstudy.springboot.consumer;

import com.example.rocketmqstudy.springboot.model.OrderMessage;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 按队列顺序消费同一订单的状态变化消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.orderly-topic}",
        consumerGroup = "${study.rocketmq.orderly-consumer-group}",
        consumeMode = ConsumeMode.ORDERLY)
public class OrderlyMessageListener implements RocketMQListener<OrderMessage> {

    /**
     * 输出订单状态变化及其步骤序号。
     *
     * @param message 订单状态消息。
     */
    @Override
    public void onMessage(OrderMessage message) {
        System.out.printf("顺序消费者收到：orderNumber=%s, sequence=%d, status=%s%n",
                message.getOrderNumber(), message.getSequence(), message.getStatus());
    }
}

package com.example.rocketmqstudy.springboot.consumer;

import com.example.rocketmqstudy.springboot.model.OrderMessage;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 消费由 Spring 消息转换器反序列化的订单对象消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.order-topic}",
        consumerGroup = "${study.rocketmq.order-consumer-group}")
public class OrderMessageListener implements RocketMQListener<OrderMessage> {

    /**
     * 输出收到的订单对象。
     *
     * @param message 订单消息。
     */
    @Override
    public void onMessage(OrderMessage message) {
        System.out.println("对象消息消费者收到：" + message);
    }
}

package com.example.rocketmqstudy.springboot.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 消费 RocketMQTemplate 发送的简单字符串消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.simple-topic}",
        consumerGroup = "${study.rocketmq.simple-consumer-group}")
public class SimpleMessageListener implements RocketMQListener<String> {

    /**
     * 输出收到的字符串消息。
     *
     * @param message 字符串消息正文。
     */
    @Override
    public void onMessage(String message) {
        System.out.println("简单消息消费者收到：" + message);
    }
}

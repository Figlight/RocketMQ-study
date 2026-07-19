package com.example.rocketmqstudy.springboot.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 只消费 Tag 为 java 或 spring 的消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.tag-topic}",
        consumerGroup = "${study.rocketmq.tag-consumer-group}",
        selectorType = SelectorType.TAG,
        selectorExpression = "java || spring")
public class TagMessageListener implements RocketMQListener<String> {

    /**
     * 输出通过 Tag 过滤的消息。
     *
     * @param message 字符串消息正文。
     */
    @Override
    public void onMessage(String message) {
        System.out.println("Tag 过滤消费者收到：" + message);
    }
}

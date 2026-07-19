package com.example.rocketmqstudy.springboot.consumer;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 广播模式消费者，同组每个在线应用实例都会收到一份消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.model-topic}",
        consumerGroup = "${study.rocketmq.broadcasting-consumer-group}",
        messageModel = MessageModel.BROADCASTING)
public class BroadcastingMessageListener implements RocketMQListener<String> {

    /** 当前应用实例端口，用于区分两个演示实例的日志。 */
    private final int instancePort;

    /**
     * 创建广播模式监听器。
     *
     * @param instancePort 当前应用实例端口。
     */
    public BroadcastingMessageListener(@Value("${server.port}") int instancePort) {
        this.instancePort = instancePort;
    }

    /**
     * 输出当前实例收到的广播消息。
     *
     * @param message 字符串消息正文。
     */
    @Override
    public void onMessage(String message) {
        System.out.printf("广播消费者收到：instancePort=%s, message=%s%n",
                instancePort, message);
    }
}

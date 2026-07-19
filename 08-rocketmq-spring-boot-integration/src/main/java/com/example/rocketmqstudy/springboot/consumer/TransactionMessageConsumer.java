package com.example.rocketmqstudy.springboot.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 只接收本地事务最终提交成功的事务消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${study.rocketmq.transaction-topic}",
        consumerGroup = "${study.rocketmq.transaction-consumer-group}")
public class TransactionMessageConsumer implements RocketMQListener<String> {

    /**
     * 输出已提交的事务消息。
     *
     * @param message 事务消息正文。
     */
    @Override
    public void onMessage(String message) {
        System.out.println("事务消息消费者收到已提交消息：" + message);
    }
}

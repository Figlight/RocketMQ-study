package com.example.rocketmqstudy.messagemode.producer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.List;

/**
 * 批量消息生产者，演示一次网络请求发送多条同 Topic 消息。
 */
public final class BatchMessageProducer {

    /**
     * 工具型入口类不允许被实例化。
     */
    private BatchMessageProducer() {
    }

    /**
     * 程序入口，发送一个由三条独立消息组成的小批次。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当客户端启动或批量发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        MessageModeConfig config = MessageModeConfig.load();
        String topic = config.topicFor(MessageScenario.BATCH);
        DefaultMQProducer producer = RocketMqClientFactory.createProducer(
                config, config.producerGroup("batch"));

        try {
            producer.start();
            List<Message> messages = List.of(
                    MessageSupport.buildMessage(topic, "Batch", "batch-order-001", "批量订单 A"),
                    MessageSupport.buildMessage(topic, "Batch", "batch-order-002", "批量订单 B"),
                    MessageSupport.buildMessage(topic, "Batch", "batch-order-003", "批量订单 C")
            );
            SendResult sendResult = producer.send(messages);
            System.out.println("批量发送完成，消息条数=" + messages.size());
            System.out.println("批量发送结果：" + sendResult);
        } finally {
            producer.shutdown();
        }
    }
}

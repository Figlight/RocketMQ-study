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
 * Tag 与 Key 示例生产者，演示消息分类订阅和业务消息定位。
 */
public final class FilterMessageProducer {

    /**
     * 工具型入口类不允许被实例化。
     */
    private FilterMessageProducer() {
    }

    /**
     * 程序入口，向同一个 Topic 发送三个不同 Tag 且带业务 Key 的消息。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当客户端启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        MessageModeConfig config = MessageModeConfig.load();
        String topic = config.topicFor(MessageScenario.FILTER);
        DefaultMQProducer producer = RocketMqClientFactory.createProducer(
                config, config.producerGroup("filter"));

        try {
            producer.start();
            for (Message message : buildMessages(topic)) {
                SendResult sendResult = producer.send(message);
                System.out.printf("tag=%s, key=%s, status=%s, msgId=%s%n",
                        message.getTags(), message.getKeys(), sendResult.getSendStatus(), sendResult.getMsgId());
            }
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 构建用于 Tag 过滤和 Key 查询的消息集合。
     *
     * @param topic 消息 Topic。
     * @return 三个不同 Tag 的消息。
     */
    private static List<Message> buildMessages(String topic) {
        return List.of(
                MessageSupport.buildMessage(topic, "TagA", "order-2001", "TagA：电器订单已创建"),
                MessageSupport.buildMessage(topic, "TagB", "order-2002", "TagB：女装订单已创建"),
                MessageSupport.buildMessage(topic, "TagC", "order-2003", "TagC：化妆品订单已创建")
        );
    }
}

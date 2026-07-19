package com.example.rocketmqstudy.messagemode.producer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.time.Instant;

/**
 * 延迟消息生产者，演示 RocketMQ 4.x 固定延迟等级的使用方式。
 */
public final class DelayMessageProducer {

    /**
     * 默认延迟等级 3，对应约 10 秒延迟。
     */
    private static final int DEFAULT_DELAY_LEVEL = 3;

    /**
     * 工具型入口类不允许被实例化。
     */
    private DelayMessageProducer() {
    }

    /**
     * 程序入口，发送一条带固定延迟等级的订单超时检查消息。
     *
     * @param args 第一个参数可选，用于指定 1 至 18 的延迟等级。
     * @throws Exception 当客户端启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        int delayLevel = parseDelayLevel(args);
        MessageModeConfig config = MessageModeConfig.load();
        String topic = config.topicFor(MessageScenario.DELAY);
        DefaultMQProducer producer = RocketMqClientFactory.createProducer(
                config, config.producerGroup("delay"));

        try {
            producer.start();
            Message message = MessageSupport.buildMessage(
                    topic, "OrderTimeout", "order-1001", "检查订单 1001 是否已付款");
            message.setDelayTimeLevel(delayLevel);
            SendResult sendResult = producer.send(message);
            System.out.println("发送时间=" + Instant.now() + "，delayLevel=" + delayLevel);
            System.out.println("延迟消息发送结果：" + sendResult);
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 解析并校验延迟等级。
     *
     * @param args 命令行参数。
     * @return 1 至 18 的延迟等级。
     */
    private static int parseDelayLevel(String[] args) {
        int delayLevel = args.length == 0 ? DEFAULT_DELAY_LEVEL : Integer.parseInt(args[0]);
        if (delayLevel < 1 || delayLevel > 18) {
            throw new IllegalArgumentException("RocketMQ 4.x 延迟等级必须在 1 至 18 之间");
        }
        return delayLevel;
    }
}

package com.example.rocketmqstudy.quickstart.producer;

import com.example.rocketmqstudy.quickstart.config.QuickStartConfig;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

/**
 * RocketMQ 快速入门生产者，用于演示同步发送消息。
 */
public final class QuickStartProducer {

    /**
     * 默认发送消息条数。
     */
    private static final int DEFAULT_MESSAGE_COUNT = 10;

    /**
     * 工具类示例不需要被实例化。
     */
    private QuickStartProducer() {
    }

    /**
     * 程序入口，创建生产者并同步发送多条消息。
     *
     * @param args 第一个参数可选，用于指定发送消息条数。
     * @throws Exception 当生产者启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        // 加载运行配置（初始化生产者组、主题、标签、NameServer 地址等）
        QuickStartConfig config = QuickStartConfig.load();
        int messageCount = parseMessageCount(args);

        // 创建生产者实例并设置 NameServer 地址
        DefaultMQProducer producer = new DefaultMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getNamesrvAddr());

        // 启动生产者
        try {
            producer.start();
            System.out.println("生产者启动成功：" + config);

            for (int index = 0; index < messageCount; index++) {
                Message message = buildMessage(config, index);
                SendResult sendResult = producer.send(message);
                System.out.println("发送结果：" + sendResult);
            }
        } finally {
            producer.shutdown();
            System.out.println("生产者已关闭");
        }
    }

    /**
     * 解析命令行传入的消息条数。
     *
     * @param args 命令行参数。
     * @return 需要发送的消息条数。
     */
    private static int parseMessageCount(String[] args) {
        if (args.length == 0 || args[0] == null || args[0].isBlank()) {
            return DEFAULT_MESSAGE_COUNT;
        }
        return Integer.parseInt(args[0]);
    }

    /**
     * 构建一条 RocketMQ 消息。
     *
     * @param config 快速入门配置。
     * @param index 当前消息序号。
     * @return RocketMQ 消息对象。
     */
    private static Message buildMessage(QuickStartConfig config, int index) {
        String key = "quickstart-key-" + index;
        String body = "Hello RocketMQ " + index;
        return new Message(
                config.getTopic(),
                config.getTagExpression(),
                key,
                body.getBytes(StandardCharsets.UTF_8)
        );
    }
}

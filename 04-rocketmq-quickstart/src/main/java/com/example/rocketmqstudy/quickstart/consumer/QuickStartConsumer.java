package com.example.rocketmqstudy.quickstart.consumer;

import com.example.rocketmqstudy.quickstart.config.QuickStartConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * RocketMQ 快速入门消费者，用于演示订阅 Topic 并监听消息。
 */
public final class QuickStartConsumer {

    /**
     * 工具类示例不需要被实例化。
     */
    private QuickStartConsumer() {
    }

    /**
     * 程序入口，创建 PushConsumer 并注册并发消费监听器。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当消费者启动失败或主线程等待被中断时抛出。
     */
    public static void main(String[] args) throws Exception {
        // 加载运行配置（初始化消费者组、主题、标签、NameServer 地址等）
        QuickStartConfig config = QuickStartConfig.load();

        // 创建消费者实例并设置 NameServer 地址
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        // 设置消费者从第一个偏移量开始消费
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        // 设置消费者从第一个偏移量开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅指定 Topic 和标签的消息
        consumer.subscribe(config.getTopic(), config.getTagExpression());
        // 注册消息监听器
        consumer.registerMessageListener(new QuickStartMessageListener(config.isForceRetry()));

        consumer.start();
        System.out.println("消费者启动成功：" + config);
        System.out.println("按 Ctrl+C 停止消费者");

        // 等待主进程被中断，确保消费者能够正常关闭
        new CountDownLatch(1).await();
    }

    /**
     * 快速入门消息监听器，负责打印消息并返回消费状态。
     */
    private static final class QuickStartMessageListener implements MessageListenerConcurrently {

        /**
         * 是否强制返回 RECONSUME_LATER。
         */
        private final boolean forceRetry;

        /**
         * 创建快速入门消息监听器。
         *
         * @param forceRetry 是否强制演示消费重试。
         */
        private QuickStartMessageListener(boolean forceRetry) {
            this.forceRetry = forceRetry;
        }

        /**
         * 消费 RocketMQ 推送过来的消息。
         *
         * @param messages 本次批量推送的消息列表。
         * @param context 并发消费上下文。
         * @return 消费成功或稍后重试。
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
                // 打印消息中的关键字段 以便观察 Topic、Tag、Key 和消息体
                printMessage(message);
            }

            // 如果开启了强制消费重试，返回 RECONSUME_LATER 状态，模拟消费失败
            if (forceRetry) {
                System.out.println("当前开启了重试演示，本批消息返回 RECONSUME_LATER");
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        /**
         * 打印消息中的关键字段，便于观察 Topic、Tag、Key 和消息体。
         *
         * @param message RocketMQ 扩展消息对象。
         */
        private void printMessage(MessageExt message) {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            System.out.printf(
                    "线程=%s, topic=%s, tag=%s, key=%s, queueId=%d, queueOffset=%d, body=%s%n",
                    Thread.currentThread().getName(),
                    message.getTopic(),
                    message.getTags(),
                    message.getKeys(),
                    message.getQueueId(),
                    message.getQueueOffset(),
                    body
            );
        }
    }
}

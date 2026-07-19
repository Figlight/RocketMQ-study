package com.example.rocketmqstudy.retrydlq.consumer;

import com.example.rocketmqstudy.retrydlq.config.RetryAndDlqConfig;
import com.example.rocketmqstudy.retrydlq.support.ConsumerLifecycle;
import com.example.rocketmqstudy.retrydlq.support.MessageSupport;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 独立死信消费者，订阅持续失败消费者组对应的死信 Topic。
 */
public final class DeadLetterConsumer {

    /** 死信处理消费者自身的组名。 */
    private static final String DLQ_HANDLER_GROUP = "retry-dlq-handler-group";

    /** 工具型入口类不允许实例化。 */
    private DeadLetterConsumer() {
    }

    /**
     * 启动死信消费者。
     *
     * @param args 未使用的命令行参数。
     * @throws Exception 消费者启动或等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        RetryAndDlqConfig config = RetryAndDlqConfig.load();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(DLQ_HANDLER_GROUP);
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(config.getDeadLetterTopic(), "*");
        consumer.registerMessageListener(new DeadLetterMessageListener());
        consumer.start();
        ConsumerLifecycle.awaitShutdown(
                "死信消费者已启动，订阅=" + config.getDeadLetterTopic(), consumer::shutdown);
    }

    /**
     * 打印并确认死信的监听器。
     */
    private static final class DeadLetterMessageListener implements MessageListenerConcurrently {

        /**
         * 处理死信；示例以打印代表人工补偿入口处理完成。
         *
         * @param messages 本次死信消息。
         * @param context 消费上下文。
         * @return 打印完成后返回消费成功。
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
                System.out.println("收到死信，进入告警/人工补偿入口：");
                MessageSupport.printMessage(message);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}

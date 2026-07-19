package com.example.rocketmqstudy.retrydlq.consumer;

import com.example.rocketmqstudy.retrydlq.config.RetryAndDlqConfig;
import com.example.rocketmqstudy.retrydlq.exception.RecoverableBusinessException;
import com.example.rocketmqstudy.retrydlq.service.RetryBusinessService;
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
 * 可恢复重试消费者，演示前两次失败、第三次成功。
 */
public final class RecoverableRetryConsumer {

    /** 工具型入口类不允许实例化。 */
    private RecoverableRetryConsumer() {
    }

    /**
     * 启动可恢复重试消费者。
     *
     * @param args 未使用的命令行参数。
     * @throws Exception 消费者启动或等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        RetryAndDlqConfig config = RetryAndDlqConfig.load();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getRecoverableConsumerGroup());
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(config.getMaxReconsumeTimes());
        consumer.subscribe(config.getTopic(), "RetrySuccess");
        consumer.registerMessageListener(new ThirdDeliverySuccessListener(new RetryBusinessService()));
        consumer.start();
        ConsumerLifecycle.awaitShutdown(
                "可恢复消费者已启动，前两次失败、第三次成功，group=" + config.getRecoverableConsumerGroup(),
                consumer::shutdown);
    }

    /**
     * 第三次投递成功的监听器。 手动模拟订单业务处理失败，重试订单业务处理成功。
     */
    private static final class ThirdDeliverySuccessListener implements MessageListenerConcurrently {

        /** 重试业务服务。 */
        private final RetryBusinessService businessService;

        /**
         * 创建监听器。
         *
         * @param businessService 重试业务服务。
         */
        private ThirdDeliverySuccessListener(RetryBusinessService businessService) {
            this.businessService = businessService;
        }

        /**
         * 逐条处理消息；业务成功才返回成功，业务失败返回稍后重试。
         *
         * @param messages 本次投递消息。
         * @param context 消费上下文。
         * @return 消费状态。
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
                MessageSupport.printMessage(message);
                try {
                    businessService.processUntilThirdDelivery(message);
                } catch (RecoverableBusinessException exception) {
                    System.out.printf("可恢复失败：%s，reconsumeTimes=%d，返回 RECONSUME_LATER%n",
                            exception.getMessage(), message.getReconsumeTimes());
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}

package com.example.rocketmqstudy.idempotency.consumer;

import com.example.rocketmqstudy.idempotency.config.IdempotencyConfig;
import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;
import com.example.rocketmqstudy.idempotency.producer.DuplicateMessageProducer;
import com.example.rocketmqstudy.idempotency.service.OrderRewardService;
import com.example.rocketmqstudy.idempotency.support.ConsumerLifecycle;
import com.example.rocketmqstudy.idempotency.support.MessageSupport;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 无幂等保护消费者，相同业务消息每次到达都会产生一次业务副作用。
 */
public final class NonIdempotentConsumer {

    /** 工具型入口类不允许实例化。 */
    private NonIdempotentConsumer() {
    }

    /**
     * 启动无幂等保护消费者。
     *
     * @param args 未使用的命令行参数。
     * @throws Exception 消费者启动或等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        IdempotencyConfig config = IdempotencyConfig.load();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getNonIdempotentConsumerGroup());
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(config.getMaxReconsumeTimes());
        consumer.subscribe(config.getTopic(), "*");
        consumer.registerMessageListener(new NonIdempotentListener(new OrderRewardService()));
        consumer.start();
        ConsumerLifecycle.awaitShutdown(
                "无幂等保护消费者已启动，group=" + config.getNonIdempotentConsumerGroup(),
                consumer::shutdown);
    }

    /**
     * 每次投递都直接执行业务的监听器。
     */
    private static final class NonIdempotentListener implements MessageListenerConcurrently {

        /** 订单积分业务服务。 */
        private final OrderRewardService rewardService;

        /**
         * 创建无幂等监听器。
         *
         * @param rewardService 订单积分业务服务。
         */
        private NonIdempotentListener(OrderRewardService rewardService) {
            this.rewardService = rewardService;
        }

        /**
         * 逐条执行业务；RetryAfterSuccess 首次投递在业务成功后故意返回失败。
         *
         * @param messages 本次回调收到的消息。
         * @param context 并发消费上下文。
         * @return 整批消息的消费结果。
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
                MessageSupport.printMessage(message);
                OrderPaidEvent event = MessageSupport.parseEvent(message);
                rewardService.grantRewardPoints(event);
                if (shouldSimulateAckFailure(message)) {
                    System.out.println("业务已成功，但模拟进程异常/确认失败，返回 RECONSUME_LATER");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        /**
         * 判断是否需要在首次投递完成业务后模拟确认失败。
         *
         * @param message 当前消息。
         * @return 首次投递的 RetryAfterSuccess 消息返回 true。
         */
        private boolean shouldSimulateAckFailure(MessageExt message) {
            return DuplicateMessageProducer.RETRY_AFTER_SUCCESS_TAG.equals(message.getTags())
                    && message.getReconsumeTimes() == 0;
        }
    }
}

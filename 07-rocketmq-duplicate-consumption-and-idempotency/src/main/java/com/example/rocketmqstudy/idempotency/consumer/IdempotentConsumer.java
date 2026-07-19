package com.example.rocketmqstudy.idempotency.consumer;

import com.example.rocketmqstudy.idempotency.config.IdempotencyConfig;
import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;
import com.example.rocketmqstudy.idempotency.model.ProcessingResult;
import com.example.rocketmqstudy.idempotency.producer.DuplicateMessageProducer;
import com.example.rocketmqstudy.idempotency.repository.InMemoryIdempotencyRecordRepository;
import com.example.rocketmqstudy.idempotency.service.IdempotentMessageProcessor;
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
 * 幂等消费者，相同业务 Key 多次到达时只执行一次业务副作用。
 */
public final class IdempotentConsumer {

    /** 工具型入口类不允许实例化。 */
    private IdempotentConsumer() {
    }

    /**
     * 启动幂等消费者。
     *
     * @param args 未使用的命令行参数。
     * @throws Exception 消费者启动或等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        IdempotencyConfig config = IdempotencyConfig.load();
        // 配置幂等消息处理编排器
        IdempotentMessageProcessor processor = new IdempotentMessageProcessor(
                new InMemoryIdempotencyRecordRepository(), new OrderRewardService());

        // 配置幂等消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getIdempotentConsumerGroup());
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(config.getMaxReconsumeTimes());
        consumer.subscribe(config.getTopic(), "*");
        // 注册监听器
        consumer.registerMessageListener(new IdempotentListener(processor));

        // 启动消费者
        consumer.start();
        ConsumerLifecycle.awaitShutdown(
                "幂等消费者已启动，内存记录仅用于学习，group=" + config.getIdempotentConsumerGroup(),
                consumer::shutdown);
    }

    /**
     * 将 RocketMQ 回调适配到幂等处理编排器的监听器。
     */
    private static final class IdempotentListener implements MessageListenerConcurrently {

        /** 幂等消息处理编排器。 */
        private final IdempotentMessageProcessor processor;

        /**
         * 创建幂等监听器。
         *
         * @param processor 幂等消息处理编排器。
         */
        private IdempotentListener(IdempotentMessageProcessor processor) {
            this.processor = processor;
        }

        /**
         * 逐条执行幂等处理，并将处理中冲突转换为整批稍后重试。
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

                // 处理幂等消息
                ProcessingResult result = processor.process(event);

                // 处理中冲突转换为整批稍后重试
                if (result == ProcessingResult.RETRY_LATER) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                if (result == ProcessingResult.EXECUTED && shouldSimulateAckFailure(message)) {
                    System.out.println("业务与幂等记录已成功，但模拟确认失败，返回 RECONSUME_LATER");
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

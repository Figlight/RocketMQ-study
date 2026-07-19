package com.example.rocketmqstudy.retrydlq.consumer;

import com.example.rocketmqstudy.retrydlq.config.RetryAndDlqConfig;
import com.example.rocketmqstudy.retrydlq.exception.RecoverableBusinessException;
import com.example.rocketmqstudy.retrydlq.exception.UnrecoverableBusinessException;
import com.example.rocketmqstudy.retrydlq.model.FailureType;
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
 * 持续失败消费者，演示达到最大重试次数或不可恢复失败后进入死信队列。
 */
public final class AlwaysFailingConsumer {

    /** 直接交给 Broker 转入死信队列的延迟级别特殊值。 */
    private static final int SEND_TO_DLQ_IMMEDIATELY = -1;

    /** 工具型入口类不允许实例化。 */
    private AlwaysFailingConsumer() {
    }

    /**
     * 启动持续失败消费者。
     *
     * @param args 第一个参数为 recoverable 或 unrecoverable。
     * @throws Exception 消费者启动或等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        FailureType failureType = FailureType.fromArguments(args);
        RetryAndDlqConfig config = RetryAndDlqConfig.load();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getDlqConsumerGroup());
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(config.getMaxReconsumeTimes());
        String tag = failureType == FailureType.RECOVERABLE
                ? "AlwaysFailRecoverable" : "AlwaysFailUnrecoverable";
        consumer.subscribe(config.getTopic(), tag);
        consumer.registerMessageListener(new AlwaysFailingListener(failureType, new RetryBusinessService()));
        consumer.start();
        ConsumerLifecycle.awaitShutdown(
                "持续失败消费者已启动，type=" + failureType.getArgumentName()
                        + "，maxReconsumeTimes=" + config.getMaxReconsumeTimes()
                        + "，dlqTopic=" + config.getDeadLetterTopic(),
                consumer::shutdown);
    }

    /**
     * 按异常类型选择重试或直接进入死信队列的监听器。
     */
    private static final class AlwaysFailingListener implements MessageListenerConcurrently {

        /** 当前演示的失败类型。 */
        private final FailureType failureType;

        /** 重试业务服务。 */
        private final RetryBusinessService businessService;

        /**
         * 创建持续失败监听器。
         *
         * @param failureType 失败类型。
         * @param businessService 重试业务服务。
         */
        private AlwaysFailingListener(FailureType failureType, RetryBusinessService businessService) {
            this.failureType = failureType;
            this.businessService = businessService;
        }

        /**
         * 执行业务并根据异常性质返回正确状态。
         *
         * @param messages 本次投递消息。
         * @param context 消费上下文。
         * @return 固定为失败状态，由 Broker 重试或转入 DLQ。
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
                MessageSupport.printMessage(message);
                try {
                    businessService.processAlwaysFail(failureType);
                } catch (UnrecoverableBusinessException exception) {
                    // RocketMQ 4.x 中 -1 表示本次发送回 Broker 时直接进入该组 DLQ。
                    context.setDelayLevelWhenNextConsume(SEND_TO_DLQ_IMMEDIATELY);
                    System.out.println("不可恢复失败：" + exception.getMessage() + "，直接进入 DLQ");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } catch (RecoverableBusinessException exception) {
                    System.out.printf("可恢复但持续失败：%s，reconsumeTimes=%d，等待 Broker 重试%n",
                            exception.getMessage(), message.getReconsumeTimes());
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}

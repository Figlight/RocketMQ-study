package com.example.rocketmqstudy.springboot.producer;

import com.example.rocketmqstudy.springboot.config.StudyRocketMqProperties;
import com.example.rocketmqstudy.springboot.model.OrderMessage;
import com.example.rocketmqstudy.springboot.model.TransactionScenario;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * 基于 RocketMQTemplate 的消息发送服务。
 */
@Service
public class RocketMqMessageProducer {

    /** 延迟消息发送超时时间，单位为毫秒。 */
    private static final long DELAY_SEND_TIMEOUT_MILLIS = 3_000L;

    /** RocketMQ Spring 提供的统一操作模板。 */
    private final RocketMQTemplate rocketMQTemplate;

    /** 学习示例 Topic 配置。 */
    private final StudyRocketMqProperties properties;

    /**
     * 创建消息发送服务。
     *
     * @param rocketMQTemplate RocketMQ 操作模板。
     * @param properties 学习示例配置。
     */
    public RocketMqMessageProducer(RocketMQTemplate rocketMQTemplate,
                                   StudyRocketMqProperties properties) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.properties = properties;
    }

    /**
     * 同步发送字符串消息并等待 Broker 确认。
     *
     * @param body 消息正文。
     * @return RocketMQ 发送结果。
     */
    public SendResult sendSynchronously(String body) {
        return rocketMQTemplate.syncSend(properties.getSimpleTopic(), body);
    }

    /**
     * 异步发送字符串消息，结果由回调处理。
     *
     * @param body 消息正文。
     */
    public void sendAsynchronously(String body) {
        rocketMQTemplate.asyncSend(properties.getSimpleTopic(), body, new LoggingSendCallback(body));
    }

    /**
     * 单向发送字符串消息，不等待 Broker 返回发送结果。
     *
     * @param body 消息正文。
     */
    public void sendOneWay(String body) {
        rocketMQTemplate.sendOneWay(properties.getSimpleTopic(), body);
    }

    /**
     * 按 RocketMQ 固定延迟等级发送消息。
     *
     * @param body 消息正文。
     * @param delayLevel RocketMQ 延迟等级。
     * @return RocketMQ 发送结果。
     */
    public SendResult sendDelayed(String body, int delayLevel) {
        if (delayLevel < 1 || delayLevel > 18) {
            throw new IllegalArgumentException("RocketMQ 4.x 延迟等级必须在 1 到 18 之间");
        }
        Message<String> message = MessageBuilder.withPayload(body).build();
        return rocketMQTemplate.syncSend(
                properties.getSimpleTopic(), message, DELAY_SEND_TIMEOUT_MILLIS, delayLevel);
    }

    /**
     * 发送可由 Spring 消息转换器序列化的订单对象。
     *
     * @param orderMessage 订单消息。
     * @return RocketMQ 发送结果。
     */
    public SendResult sendOrder(OrderMessage orderMessage) {
        return rocketMQTemplate.syncSend(properties.getOrderTopic(), orderMessage);
    }

    /**
     * 按订单号选择队列，依次发送同一订单的状态变化。
     *
     * @param orderNumber 订单号。
     * @param userId 用户编号。
     */
    public void sendOrderLifecycle(String orderNumber, String userId) {
        if (orderNumber == null || orderNumber.isBlank()) {
            throw new IllegalArgumentException("订单号不能为空");
        }
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("用户编号不能为空");
        }
        List<OrderMessage> lifecycle = List.of(
                new OrderMessage(orderNumber, userId, "CREATED", 1),
                new OrderMessage(orderNumber, userId, "PAID", 2),
                new OrderMessage(orderNumber, userId, "SHIPPED", 3),
                new OrderMessage(orderNumber, userId, "RECEIVED", 4));
        for (OrderMessage orderMessage : lifecycle) {
            rocketMQTemplate.syncSendOrderly(
                    properties.getOrderlyTopic(), orderMessage, orderMessage.getOrderNumber());
        }
    }

    /**
     * 发送带 Tag 的字符串消息。
     *
     * @param tag 消息 Tag。
     * @param body 消息正文。
     * @return RocketMQ 发送结果。
     */
    public SendResult sendTagged(String tag, String body) {
        if (tag == null || tag.isBlank()) {
            throw new IllegalArgumentException("消息 Tag 不能为空");
        }
        return rocketMQTemplate.syncSend(properties.getTagTopic() + ":" + tag, body);
    }

    /**
     * 发送事务半消息，并把演示场景传给本地事务监听器。
     *
     * @param body 消息正文。
     * @param scenario 本地事务场景。
     * @return 事务消息发送结果。
     */
    public TransactionSendResult sendTransaction(String body, TransactionScenario scenario) {
        String transactionKey = "TX-" + UUID.randomUUID();
        Message<String> message = MessageBuilder.withPayload(body)
                .setHeader(RocketMQHeaders.KEYS, transactionKey)
                .build();
        return rocketMQTemplate.sendMessageInTransaction(
                properties.getTransactionTopic(), message, scenario);
    }

    /**
     * 向消费模式演示 Topic 连续发送消息。
     *
     * @param count 消息数量。
     */
    public void sendConsumerModelMessages(int count) {
        if (count < 1 || count > 100) {
            throw new IllegalArgumentException("消息数量必须在 1 到 100 之间");
        }
        for (int index = 1; index <= count; index++) {
            rocketMQTemplate.syncSend(properties.getModelTopic(), "消费模式演示消息-" + index);
        }
    }

    /**
     * 将异步发送结果输出到控制台的回调。
     */
    private static final class LoggingSendCallback implements SendCallback {

        /** 原始消息正文，用于关联回调日志。 */
        private final String body;

        /**
         * 创建异步发送回调。
         *
         * @param body 原始消息正文。
         */
        private LoggingSendCallback(String body) {
            this.body = body;
        }

        /**
         * 输出异步发送成功结果。
         *
         * @param sendResult RocketMQ 发送结果。
         */
        @Override
        public void onSuccess(SendResult sendResult) {
            System.out.printf("异步发送成功：body=%s, msgId=%s, status=%s%n",
                    body, sendResult.getMsgId(), sendResult.getSendStatus());
        }

        /**
         * 输出异步发送失败原因。
         *
         * @param throwable 发送异常。
         */
        @Override
        public void onException(Throwable throwable) {
            System.err.printf("异步发送失败：body=%s, error=%s%n", body, throwable.getMessage());
        }
    }
}

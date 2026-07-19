package com.example.rocketmqstudy.springboot.producer;

import com.example.rocketmqstudy.springboot.config.StudyRocketMqProperties;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RocketMqMessageProducer 的轻量单元测试，不依赖真实 Broker。
 */
class RocketMqMessageProducerTest {

    /** 模拟的 RocketMQTemplate。 */
    private RocketMQTemplate rocketMQTemplate;

    /** 被测试的消息发送服务。 */
    private RocketMqMessageProducer messageProducer;

    /**
     * 创建测试依赖和集中配置。
     */
    @BeforeEach
    void setUp() {
        rocketMQTemplate = mock(RocketMQTemplate.class);
        StudyRocketMqProperties properties = new StudyRocketMqProperties();
        properties.setSimpleTopic("SimpleTopic");
        properties.setOrderTopic("OrderTopic");
        properties.setOrderlyTopic("OrderlyTopic");
        properties.setTagTopic("TagTopic");
        properties.setTransactionTopic("TransactionTopic");
        properties.setModelTopic("ModelTopic");
        messageProducer = new RocketMqMessageProducer(rocketMQTemplate, properties);
    }

    /**
     * 验证同步发送使用配置中的简单消息 Topic。
     */
    @Test
    void shouldSendSynchronousMessageToConfiguredTopic() {
        SendResult expectedResult = new SendResult();
        when(rocketMQTemplate.syncSend("SimpleTopic", "hello")).thenReturn(expectedResult);

        messageProducer.sendSynchronously("hello");

        verify(rocketMQTemplate).syncSend("SimpleTopic", "hello");
    }

    /**
     * 验证延迟等级超出 RocketMQ 4.x 固定范围时拒绝发送。
     */
    @Test
    void shouldRejectUnsupportedDelayLevel() {
        assertThrows(IllegalArgumentException.class,
                () -> messageProducer.sendDelayed("delay", 19));
    }

    /**
     * 验证消费模式消息数量受到安全边界限制。
     */
    @Test
    void shouldRejectTooManyConsumerModelMessages() {
        assertThrows(IllegalArgumentException.class,
                () -> messageProducer.sendConsumerModelMessages(101));
    }
}

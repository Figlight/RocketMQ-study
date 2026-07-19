package com.example.rocketmqstudy.idempotency.producer;

import com.example.rocketmqstudy.idempotency.config.IdempotencyConfig;
import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;
import com.example.rocketmqstudy.idempotency.support.MessageSupport;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Locale;

/**
 * 重复消息生产者，演示显式重复发送和消费成功后确认失败两种场景。
 */
public final class DuplicateMessageProducer {

    /** 默认订单号，保证不同发送尝试可以使用相同业务身份。 */
    private static final String DEFAULT_ORDER_NUMBER = "ORDER-20260715-001";

    /** 默认用户编号。 */
    private static final String DEFAULT_USER_ID = "USER-1001";

    /** 默认奖励积分。 */
    private static final int DEFAULT_REWARD_POINTS = 100;

    /** 显式重复发送场景的 Tag。 */
    public static final String DUPLICATE_TAG = "ExplicitDuplicate";

    /** 业务成功后模拟确认失败场景的 Tag。 */
    public static final String RETRY_AFTER_SUCCESS_TAG = "RetryAfterSuccess";

    /** 工具型入口类不允许实例化。 */
    private DuplicateMessageProducer() {
    }

    /**
     * 启动生产者并发送演示消息。
     *
     * @param args 第一个参数为 duplicate 或 retry，第二个参数可覆盖订单号。
     * @throws Exception 生产者启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        DemoScenario scenario = DemoScenario.fromArguments(args);
        String orderNumber = args.length >= 2 ? args[1] : DEFAULT_ORDER_NUMBER;
        OrderPaidEvent event = new OrderPaidEvent(orderNumber, DEFAULT_USER_ID, DEFAULT_REWARD_POINTS);
        IdempotencyConfig config = IdempotencyConfig.load();
        DefaultMQProducer producer = new DefaultMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getNamesrvAddr());

        try {
            producer.start();
            int sendCount = scenario == DemoScenario.DUPLICATE ? 2 : 1;
            for (int index = 1; index <= sendCount; index++) {
                Message message = MessageSupport.buildMessage(config.getTopic(), scenario.tag, event);
                SendResult sendResult = producer.send(message);
                System.out.printf("发送完成：scenario=%s, sequence=%d, businessKey=%s, msgId=%s%n",
                        scenario.argumentName, index, event.businessKey(), sendResult.getMsgId());
            }
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 生产者演示场景。
     */
    private enum DemoScenario {

        /** 主动发送两条业务身份相同、msgId 不同的消息。 */
        DUPLICATE("duplicate", DUPLICATE_TAG),

        /** 发送一条由消费者模拟确认失败并触发重试的消息。 */
        RETRY("retry", RETRY_AFTER_SUCCESS_TAG);

        /** 命令行参数值。 */
        private final String argumentName;

        /** 消息 Tag。 */
        private final String tag;

        /**
         * 创建演示场景。
         *
         * @param argumentName 命令行参数值。
         * @param tag 消息 Tag。
         */
        DemoScenario(String argumentName, String tag) {
            this.argumentName = argumentName;
            this.tag = tag;
        }

        /**
         * 从命令行参数解析演示场景。
         *
         * @param args 命令行参数。
         * @return 演示场景。
         */
        private static DemoScenario fromArguments(String[] args) {
            String value = args.length == 0 ? DUPLICATE.argumentName : args[0].toLowerCase(Locale.ROOT);
            for (DemoScenario scenario : values()) {
                if (scenario.argumentName.equals(value)) {
                    return scenario;
                }
            }
            throw new IllegalArgumentException("场景只支持 duplicate 或 retry：" + value);
        }
    }
}

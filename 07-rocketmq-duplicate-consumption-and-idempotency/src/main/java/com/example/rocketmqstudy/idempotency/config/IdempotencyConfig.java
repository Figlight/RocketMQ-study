package com.example.rocketmqstudy.idempotency.config;

/**
 * 重复消费与幂等示例的集中配置。
 */
public final class IdempotencyConfig {

    /** 默认 NameServer 地址。 */
    private static final String DEFAULT_NAMESRV_ADDR = "127.0.0.1:9876";

    /** 默认业务 Topic。 */
    private static final String DEFAULT_TOPIC = "StudyDuplicateConsumptionTopic";

    /** 默认生产者组。 */
    private static final String DEFAULT_PRODUCER_GROUP = "duplicate-demo-producer-group";

    /** 无幂等保护消费者组。 */
    private static final String DEFAULT_NON_IDEMPOTENT_CONSUMER_GROUP = "non-idempotent-consumer-group";

    /** 幂等消费者组。 */
    private static final String DEFAULT_IDEMPOTENT_CONSUMER_GROUP = "idempotent-consumer-group";

    /** 默认最大重新消费次数。 */
    private static final int DEFAULT_MAX_RECONSUME_TIMES = 2;

    /** NameServer 地址。 */
    private final String namesrvAddr;

    /** 业务 Topic。 */
    private final String topic;

    /** 生产者组。 */
    private final String producerGroup;

    /** 无幂等保护消费者组。 */
    private final String nonIdempotentConsumerGroup;

    /** 幂等消费者组。 */
    private final String idempotentConsumerGroup;

    /** 最大重新消费次数。 */
    private final int maxReconsumeTimes;

    /**
     * 创建不可变配置对象。
     *
     * @param namesrvAddr NameServer 地址。
     * @param topic 业务 Topic。
     * @param producerGroup 生产者组。
     * @param nonIdempotentConsumerGroup 无幂等保护消费者组。
     * @param idempotentConsumerGroup 幂等消费者组。
     * @param maxReconsumeTimes 最大重新消费次数。
     */
    private IdempotencyConfig(String namesrvAddr, String topic, String producerGroup,
                              String nonIdempotentConsumerGroup, String idempotentConsumerGroup,
                              int maxReconsumeTimes) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.producerGroup = producerGroup;
        this.nonIdempotentConsumerGroup = nonIdempotentConsumerGroup;
        this.idempotentConsumerGroup = idempotentConsumerGroup;
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    /**
     * 按“系统属性、环境变量、默认值”的优先级加载配置。
     *
     * @return 加载完成的配置。
     */
    public static IdempotencyConfig load() {
        int maxReconsumeTimes = Integer.parseInt(readString(
                "rocketmq.idempotency.maxReconsumeTimes",
                "ROCKETMQ_IDEMPOTENCY_MAX_RECONSUME_TIMES",
                String.valueOf(DEFAULT_MAX_RECONSUME_TIMES)));
        if (maxReconsumeTimes < 0) {
            throw new IllegalArgumentException("rocketmq.idempotency.maxReconsumeTimes 不能小于 0");
        }
        return new IdempotencyConfig(
                readString("rocketmq.namesrvAddr", "ROCKETMQ_NAMESRV_ADDR", DEFAULT_NAMESRV_ADDR),
                readString("rocketmq.idempotency.topic", "ROCKETMQ_IDEMPOTENCY_TOPIC", DEFAULT_TOPIC),
                readString("rocketmq.idempotency.producerGroup", "ROCKETMQ_IDEMPOTENCY_PRODUCER_GROUP",
                        DEFAULT_PRODUCER_GROUP),
                readString("rocketmq.idempotency.nonIdempotentGroup", "ROCKETMQ_NON_IDEMPOTENT_GROUP",
                        DEFAULT_NON_IDEMPOTENT_CONSUMER_GROUP),
                readString("rocketmq.idempotency.idempotentGroup", "ROCKETMQ_IDEMPOTENT_GROUP",
                        DEFAULT_IDEMPOTENT_CONSUMER_GROUP),
                maxReconsumeTimes);
    }

    /** @return NameServer 地址。 */
    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    /** @return 业务 Topic。 */
    public String getTopic() {
        return topic;
    }

    /** @return 生产者组。 */
    public String getProducerGroup() {
        return producerGroup;
    }

    /** @return 无幂等保护消费者组。 */
    public String getNonIdempotentConsumerGroup() {
        return nonIdempotentConsumerGroup;
    }

    /** @return 幂等消费者组。 */
    public String getIdempotentConsumerGroup() {
        return idempotentConsumerGroup;
    }

    /** @return 最大重新消费次数。 */
    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    /**
     * 读取字符串配置。
     *
     * @param propertyName JVM 系统属性名。
     * @param environmentName 环境变量名。
     * @param defaultValue 默认值。
     * @return 最终配置值。
     */
    private static String readString(String propertyName, String environmentName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }
        String environmentValue = System.getenv(environmentName);
        return environmentValue == null || environmentValue.isBlank() ? defaultValue : environmentValue;
    }
}

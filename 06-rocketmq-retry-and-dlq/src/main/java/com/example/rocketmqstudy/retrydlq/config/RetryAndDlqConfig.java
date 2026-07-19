package com.example.rocketmqstudy.retrydlq.config;

/**
 * 重试与死信示例的集中配置。
 */
public final class RetryAndDlqConfig {

    /** 默认 NameServer 地址。 */
    private static final String DEFAULT_NAMESRV_ADDR = "127.0.0.1:9876";

    /** 默认业务 Topic。 */
    private static final String DEFAULT_TOPIC = "StudyRetryAndDlqTopic";

    /** 默认生产者组。 */
    private static final String DEFAULT_PRODUCER_GROUP = "retry-dlq-producer-group";

    /** “第三次成功”场景的消费者组。 */
    private static final String DEFAULT_RECOVERABLE_CONSUMER_GROUP = "retry-success-consumer-group";

    /** 持续失败及死信场景的消费者组。 */
    private static final String DEFAULT_DLQ_CONSUMER_GROUP = "retry-dlq-consumer-group";

    /** 消费失败后的最大重新消费次数。 */
    private static final int DEFAULT_MAX_RECONSUME_TIMES = 2;

    /** 同步发送失败时的重试次数。 */
    private static final int DEFAULT_SYNC_RETRY_TIMES = 3;

    /** 异步发送失败时的重试次数。 */
    private static final int DEFAULT_ASYNC_RETRY_TIMES = 3;

    /** 单次发送超时时间，单位为毫秒。 */
    private static final long DEFAULT_SEND_TIMEOUT_MILLIS = 3000L;

    /** NameServer 地址。 */
    private final String namesrvAddr;

    /** 业务 Topic。 */
    private final String topic;

    /** 生产者组。 */
    private final String producerGroup;

    /** “第三次成功”消费者组。 */
    private final String recoverableConsumerGroup;

    /** 持续失败消费者组。 */
    private final String dlqConsumerGroup;

    /** 最大重新消费次数。 */
    private final int maxReconsumeTimes;

    /** 同步发送重试次数。 */
    private final int syncRetryTimes;

    /** 异步发送重试次数。 */
    private final int asyncRetryTimes;

    /** 发送超时时间。 */
    private final long sendTimeoutMillis;

    /**
     * 创建不可变配置对象。
     *
     * @param namesrvAddr NameServer 地址。
     * @param topic 业务 Topic。
     * @param producerGroup 生产者组。
     * @param recoverableConsumerGroup “第三次成功”消费者组。
     * @param dlqConsumerGroup 持续失败消费者组。
     * @param maxReconsumeTimes 最大重新消费次数。
     * @param syncRetryTimes 同步发送重试次数。
     * @param asyncRetryTimes 异步发送重试次数。
     * @param sendTimeoutMillis 发送超时时间。
     */
    private RetryAndDlqConfig(String namesrvAddr, String topic, String producerGroup,
                              String recoverableConsumerGroup, String dlqConsumerGroup,
                              int maxReconsumeTimes, int syncRetryTimes, int asyncRetryTimes,
                              long sendTimeoutMillis) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.producerGroup = producerGroup;
        this.recoverableConsumerGroup = recoverableConsumerGroup;
        this.dlqConsumerGroup = dlqConsumerGroup;
        this.maxReconsumeTimes = maxReconsumeTimes;
        this.syncRetryTimes = syncRetryTimes;
        this.asyncRetryTimes = asyncRetryTimes;
        this.sendTimeoutMillis = sendTimeoutMillis;
    }

    /**
     * 按“系统属性、环境变量、默认值”的优先级加载配置。
     *
     * @return 加载完成的配置。
     */
    public static RetryAndDlqConfig load() {
        return new RetryAndDlqConfig(
                readString("rocketmq.namesrvAddr", "ROCKETMQ_NAMESRV_ADDR", DEFAULT_NAMESRV_ADDR),
                readString("rocketmq.retry.topic", "ROCKETMQ_RETRY_TOPIC", DEFAULT_TOPIC),
                readString("rocketmq.retry.producerGroup", "ROCKETMQ_RETRY_PRODUCER_GROUP", DEFAULT_PRODUCER_GROUP),
                readString("rocketmq.retry.successConsumerGroup", "ROCKETMQ_RETRY_SUCCESS_CONSUMER_GROUP",
                        DEFAULT_RECOVERABLE_CONSUMER_GROUP),
                readString("rocketmq.retry.dlqConsumerGroup", "ROCKETMQ_RETRY_DLQ_CONSUMER_GROUP",
                        DEFAULT_DLQ_CONSUMER_GROUP),
                readInteger("rocketmq.retry.maxReconsumeTimes", "ROCKETMQ_RETRY_MAX_RECONSUME_TIMES",
                        DEFAULT_MAX_RECONSUME_TIMES),
                readInteger("rocketmq.retry.syncTimes", "ROCKETMQ_SYNC_RETRY_TIMES", DEFAULT_SYNC_RETRY_TIMES),
                readInteger("rocketmq.retry.asyncTimes", "ROCKETMQ_ASYNC_RETRY_TIMES", DEFAULT_ASYNC_RETRY_TIMES),
                readLong("rocketmq.retry.sendTimeoutMillis", "ROCKETMQ_SEND_TIMEOUT_MILLIS",
                        DEFAULT_SEND_TIMEOUT_MILLIS));
    }

    /** @return NameServer 地址。 */
    public String getNamesrvAddr() { return namesrvAddr; }

    /** @return 业务 Topic。 */
    public String getTopic() { return topic; }

    /** @return 生产者组。 */
    public String getProducerGroup() { return producerGroup; }

    /** @return “第三次成功”消费者组。 */
    public String getRecoverableConsumerGroup() { return recoverableConsumerGroup; }

    /** @return 持续失败消费者组。 */
    public String getDlqConsumerGroup() { return dlqConsumerGroup; }

    /** @return 持续失败消费者组对应的死信 Topic。 */
    public String getDeadLetterTopic() { return "%DLQ%" + dlqConsumerGroup; }

    /** @return 最大重新消费次数。 */
    public int getMaxReconsumeTimes() { return maxReconsumeTimes; }

    /** @return 同步发送失败重试次数。 */
    public int getSyncRetryTimes() { return syncRetryTimes; }

    /** @return 异步发送失败重试次数。 */
    public int getAsyncRetryTimes() { return asyncRetryTimes; }

    /** @return 发送超时时间，单位毫秒。 */
    public long getSendTimeoutMillis() { return sendTimeoutMillis; }

    /**
     * 读取字符串配置。
     *
     * @param propertyName 系统属性名。
     * @param environmentName 环境变量名。
     * @param defaultValue 默认值。
     * @return 最终字符串值。
     */
    private static String readString(String propertyName, String environmentName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }
        String environmentValue = System.getenv(environmentName);
        return environmentValue == null || environmentValue.isBlank() ? defaultValue : environmentValue;
    }

    /**
     * 读取非负整数配置。
     *
     * @param propertyName 系统属性名。
     * @param environmentName 环境变量名。
     * @param defaultValue 默认值。
     * @return 最终整数值。
     */
    private static int readInteger(String propertyName, String environmentName, int defaultValue) {
        int value = Integer.parseInt(readString(propertyName, environmentName, String.valueOf(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException(propertyName + " 不能小于 0");
        }
        return value;
    }

    /**
     * 读取正长整数配置。
     *
     * @param propertyName 系统属性名。
     * @param environmentName 环境变量名。
     * @param defaultValue 默认值。
     * @return 最终长整数值。
     */
    private static long readLong(String propertyName, String environmentName, long defaultValue) {
        long value = Long.parseLong(readString(propertyName, environmentName, String.valueOf(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(propertyName + " 必须大于 0");
        }
        return value;
    }
}

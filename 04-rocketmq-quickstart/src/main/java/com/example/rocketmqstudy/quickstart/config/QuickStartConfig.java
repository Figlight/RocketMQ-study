package com.example.rocketmqstudy.quickstart.config;

/**
 * RocketMQ 快速入门示例的运行配置。
 */
public final class QuickStartConfig {

    /**
     * NameServer 地址，例如 127.0.0.1:9876。
     */
    private final String namesrvAddr;

    /**
     * 消息主题名称。
     */
    private final String topic;

    /**
     * 消息标签表达式，生产者发送时使用单个 Tag，消费者订阅时也使用该表达式。
     */
    private final String tagExpression;

    /**
     * 生产者组名称。
     */
    private final String producerGroup;

    /**
     * 消费者组名称。
     */
    private final String consumerGroup;

    /**
     * 是否强制返回 RECONSUME_LATER，用于演示消费重试。
     */
    private final boolean forceRetry;

    /**
     * 创建快速入门配置对象。
     *
     * @param namesrvAddr NameServer 地址。
     * @param topic 消息主题名称。
     * @param tagExpression 消息标签表达式。
     * @param producerGroup 生产者组名称。
     * @param consumerGroup 消费者组名称。
     * @param forceRetry 是否强制演示消费重试。
     */
    private QuickStartConfig(String namesrvAddr, String topic, String tagExpression,
                             String producerGroup, String consumerGroup, boolean forceRetry) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.tagExpression = tagExpression;
        this.producerGroup = producerGroup;
        this.consumerGroup = consumerGroup;
        this.forceRetry = forceRetry;
    }

    /**
     * 从系统属性和环境变量中读取运行配置。
     *
     * @return 快速入门配置对象。
     */
    public static QuickStartConfig load() {
        return new QuickStartConfig(
                readConfig("rocketmq.namesrvAddr", "ROCKETMQ_NAMESRV_ADDR", QuickStartConstants.DEFAULT_NAMESRV_ADDR),
                readConfig("rocketmq.topic", "ROCKETMQ_TOPIC", QuickStartConstants.DEFAULT_TOPIC),
                readConfig("rocketmq.tag", "ROCKETMQ_TAG", QuickStartConstants.DEFAULT_TAG),
                readConfig("rocketmq.producerGroup", "ROCKETMQ_PRODUCER_GROUP", QuickStartConstants.DEFAULT_PRODUCER_GROUP),
                readConfig("rocketmq.consumerGroup", "ROCKETMQ_CONSUMER_GROUP", QuickStartConstants.DEFAULT_CONSUMER_GROUP),
                Boolean.parseBoolean(readConfig("rocketmq.forceRetry", "ROCKETMQ_FORCE_RETRY", "false"))
        );
    }

    /**
     * 优先读取 JVM 系统属性，其次读取环境变量，最后使用默认值。
     *
     * @param propertyName JVM 系统属性名称。
     * @param envName 环境变量名称。
     * @param defaultValue 默认值。
     * @return 解析后的配置值。
     */
    private static String readConfig(String propertyName, String envName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }

        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        return defaultValue;
    }

    /**
     * 获取 NameServer 地址。
     *
     * @return NameServer 地址。
     */
    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    /**
     * 获取消息主题名称。
     *
     * @return 消息主题名称。
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获取消息标签表达式。
     *
     * @return 消息标签表达式。
     */
    public String getTagExpression() {
        return tagExpression;
    }

    /**
     * 获取生产者组名称。
     *
     * @return 生产者组名称。
     */
    public String getProducerGroup() {
        return producerGroup;
    }

    /**
     * 获取消费者组名称。
     *
     * @return 消费者组名称。
     */
    public String getConsumerGroup() {
        return consumerGroup;
    }

    /**
     * 判断是否强制演示消费重试。
     *
     * @return 如果需要返回 RECONSUME_LATER，则返回 true。
     */
    public boolean isForceRetry() {
        return forceRetry;
    }

    /**
     * 输出便于日志观察的配置摘要。
     *
     * @return 配置摘要文本。
     */
    @Override
    public String toString() {
        return "QuickStartConfig{"
                + "namesrvAddr='" + namesrvAddr + '\''
                + ", topic='" + topic + '\''
                + ", tagExpression='" + tagExpression + '\''
                + ", producerGroup='" + producerGroup + '\''
                + ", consumerGroup='" + consumerGroup + '\''
                + ", forceRetry=" + forceRetry
                + '}';
    }
}

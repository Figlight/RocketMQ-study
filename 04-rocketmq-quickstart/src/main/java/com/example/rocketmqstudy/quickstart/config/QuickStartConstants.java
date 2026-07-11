package com.example.rocketmqstudy.quickstart.config;

/**
 * RocketMQ 快速入门章节使用的公共常量。
 */
public final class QuickStartConstants {

    /**
     * 默认 NameServer 地址，对应本地 RocketMQ 的 9876 端口。
     */
    public static final String DEFAULT_NAMESRV_ADDR = "127.0.0.1:9876";

    /**
     * 默认 Topic 名称，对应课程文档中的 TopicTest。
     */
    public static final String DEFAULT_TOPIC = "TopicTest";

    /**
     * 默认 Tag 名称，用于演示消息标签过滤。
     */
    public static final String DEFAULT_TAG = "TagA";

    /**
     * 默认生产者组名称。
     */
    public static final String DEFAULT_PRODUCER_GROUP = "quickstart-producer-group";

    /**
     * 默认消费者组名称。
     */
    public static final String DEFAULT_CONSUMER_GROUP = "quickstart-consumer-group-test2";

    /**
     * 工具类不需要被实例化。
     */
    private QuickStartConstants() {
    }
}

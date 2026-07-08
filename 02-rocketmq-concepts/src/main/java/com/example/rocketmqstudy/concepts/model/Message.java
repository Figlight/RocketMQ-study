package com.example.rocketmqstudy.concepts.model;

import java.util.Objects;

/**
 * Message，代表 RocketMQ 中传递的一条业务消息。
 */
public final class Message {

    /**
     * Topic 名称，用于标识消息分类。
     */
    private final String topic;

    /**
     * 消息业务 key，用于排查问题或关联业务数据。
     */
    private final String key;

    /**
     * 消息体，用于保存业务需要传递的内容。
     */
    private final String body;

    /**
     * 创建消息。
     *
     * @param topic Topic 名称。
     * @param key 消息业务 key。
     * @param body 消息内容。
     */
    public Message(String topic, String key, String body) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.key = Objects.requireNonNull(key, "key");
        this.body = Objects.requireNonNull(body, "body");
    }

    /**
     * 获取 Topic 名称。
     *
     * @return Topic 名称。
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获取消息业务 key。
     *
     * @return 消息业务 key。
     */
    public String getKey() {
        return key;
    }

    /**
     * 获取消息体。
     *
     * @return 消息体。
     */
    public String getBody() {
        return body;
    }
}

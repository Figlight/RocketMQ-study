package com.example.rocketmqstudy.retrydlq.support;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

/**
 * 消息构建与日志输出工具。
 */
public final class MessageSupport {

    /** 工具类不允许实例化。 */
    private MessageSupport() {
    }

    /**
     * 构建 UTF-8 编码的业务消息。
     *
     * @param topic Topic。
     * @param tag Tag。
     * @param key 业务 Key。
     * @param body 消息正文。
     * @return RocketMQ 消息。
     */
    public static Message buildMessage(String topic, String tag, String key, String body) {
        return new Message(topic, tag, key, body.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 打印消费时需要观察的关键字段。
     *
     * @param message Broker 投递的消息。
     */
    public static void printMessage(MessageExt message) {
        System.out.printf("topic=%s, tag=%s, key=%s, msgId=%s, reconsumeTimes=%d, body=%s%n",
                message.getTopic(), message.getTags(), message.getKeys(), message.getMsgId(),
                message.getReconsumeTimes(), new String(message.getBody(), StandardCharsets.UTF_8));
    }
}

package com.example.rocketmqstudy.idempotency.support;

import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

/**
 * 消息构建、解析与日志输出工具。
 */
public final class MessageSupport {

    /** 工具类不允许实例化。 */
    private MessageSupport() {
    }

    /**
     * 使用稳定业务 Key 构建 UTF-8 编码的 RocketMQ 消息。
     *
     * @param topic Topic。
     * @param tag 演示场景 Tag。
     * @param event 订单支付完成事件。
     * @return RocketMQ 消息。
     */
    public static Message buildMessage(String topic, String tag, OrderPaidEvent event) {
        return new Message(topic, tag, event.businessKey(),
                event.toMessageBody().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 解析消息正文，并校验 RocketMQ Key 与正文计算出的业务 Key 一致。
     *
     * @param message Broker 投递的消息。
     * @return 解析后的业务事件。
     */
    public static OrderPaidEvent parseEvent(MessageExt message) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        OrderPaidEvent event = OrderPaidEvent.fromMessageBody(body);
        if (!event.businessKey().equals(message.getKeys())) {
            throw new IllegalArgumentException("消息 Key 与正文业务身份不一致：" + message.getKeys());
        }
        return event;
    }

    /**
     * 打印重复消费排查所需的关键消息字段。
     *
     * @param message Broker 投递的消息。
     */
    public static void printMessage(MessageExt message) {
        System.out.printf("topic=%s, tag=%s, businessKey=%s, msgId=%s, reconsumeTimes=%d%n",
                message.getTopic(), message.getTags(), message.getKeys(), message.getMsgId(),
                message.getReconsumeTimes());
    }
}

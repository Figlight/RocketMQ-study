package com.example.rocketmqstudy.messagemode.producer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.model.OrderEvent;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 顺序消息生产者，按照订单号选择队列以保证单个订单内部的流程顺序。
 */
public final class OrderedMessageProducer {

    /**
     * 工具型入口类不允许被实例化。
     */
    private OrderedMessageProducer() {
    }

    /**
     * 程序入口，交错发送两个订单的流程事件并保持各订单内部有序。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当客户端启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        MessageModeConfig config = MessageModeConfig.load();
        String topic = config.topicFor(MessageScenario.ORDERED);
        DefaultMQProducer producer = RocketMqClientFactory.createProducer(
                config, config.producerGroup("ordered"));

        try {
            producer.start();
            for (OrderEvent orderEvent : buildOrderEvents()) {
                Message message = MessageSupport.buildMessage(
                        topic,
                        "OrderFlow",
                        "order-" + orderEvent.getOrderNumber() + "-event-" + orderEvent.getEventId(),
                        orderEvent.toMessageBody()
                );
                SendResult sendResult = producer.send(message, new OrderNumberQueueSelector(),
                        orderEvent.getOrderNumber());
                System.out.printf("orderNumber=%d, step=%s, queueId=%d, msgId=%s%n",
                        orderEvent.getOrderNumber(),
                        orderEvent.getDescription(),
                        sendResult.getMessageQueue().getQueueId(),
                        sendResult.getMsgId());
            }
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 构建两个订单的交错流程事件。
     *
     * @return 待发送的订单流程事件列表。
     */
    private static List<OrderEvent> buildOrderEvents() {
        return List.of(
                new OrderEvent(1, 111L, "下订单"),
                new OrderEvent(4, 112L, "下订单"),
                new OrderEvent(2, 111L, "物流"),
                new OrderEvent(5, 112L, "物流"),
                new OrderEvent(3, 111L, "签收"),
                new OrderEvent(6, 112L, "拒收")
        );
    }

    /**
     * 根据订单号稳定选择消息队列的选择器。
     */
    private static final class OrderNumberQueueSelector implements MessageQueueSelector {

        /**
         * 使用订单号对队列数量取模，使同一订单始终进入同一队列。
         *
         * @param queues 当前 Topic 的可写队列。
         * @param message 待发送消息。
         * @param argument 调用方传入的订单号。
         * @return 为该订单选择的消息队列。
         */
        @Override
        public MessageQueue select(List<MessageQueue> queues, Message message, Object argument) {
            long orderNumber = (Long) argument;
            int queueIndex = Math.floorMod(Long.hashCode(orderNumber), queues.size());
            return queues.get(queueIndex);
        }
    }
}

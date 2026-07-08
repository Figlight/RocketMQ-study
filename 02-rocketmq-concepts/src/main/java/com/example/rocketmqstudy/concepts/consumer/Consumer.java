package com.example.rocketmqstudy.concepts.consumer;

import com.example.rocketmqstudy.concepts.broker.Broker;
import com.example.rocketmqstudy.concepts.model.Message;
import com.example.rocketmqstudy.concepts.nameserver.NameServer;

import java.util.List;

/**
 * Consumer，负责从 Broker 中拉取并处理消息。
 */
public final class Consumer {

    /**
     * 消费者名称，用于区分同组内不同消费者实例。
     */
    private final String consumerName;

    /**
     * 消费者组名称，同组消费者通常共同分摊一个 Topic 下的 Queue。
     */
    private final String consumerGroup;

    /**
     * NameServer，用于消费前查询 Broker 路由。
     */
    private final NameServer nameServer;

    /**
     * 当前消费者负责拉取的 Queue 编号。
     */
    private final int assignedQueueId;

    /**
     * 创建消费者。
     *
     * @param consumerName 消费者名称。
     * @param consumerGroup 消费者组名称。
     * @param nameServer NameServer 实例。
     * @param assignedQueueId 当前消费者负责的 Queue 编号。
     */
    public Consumer(String consumerName, String consumerGroup, NameServer nameServer, int assignedQueueId) {
        this.consumerName = consumerName;
        this.consumerGroup = consumerGroup;
        this.nameServer = nameServer;
        this.assignedQueueId = assignedQueueId;
    }

    /**
     * 从指定 Topic 拉取并消费消息。
     *
     * @param topic Topic 名称。
     */
    public void poll(String topic) {
        Broker broker = nameServer.route(topic);
        List<Message> messages = broker.pullMessages(consumerGroup, topic, assignedQueueId);
        if (messages.isEmpty()) {
            System.out.println(consumerName + " 没有拉取到新消息");
            return;
        }
        for (Message message : messages) {
            System.out.println(consumerName + " 消费消息：group=" + consumerGroup
                    + ", queueId=" + assignedQueueId
                    + ", key=" + message.getKey()
                    + ", body=" + message.getBody());
        }
    }
}

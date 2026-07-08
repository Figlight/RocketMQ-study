package com.example.rocketmqstudy.concepts.producer;

import com.example.rocketmqstudy.concepts.broker.Broker;
import com.example.rocketmqstudy.concepts.model.Message;
import com.example.rocketmqstudy.concepts.nameserver.NameServer;

/**
 * Producer，负责创建消息并发送到 Broker。
 */
public final class Producer {

    /**
     * 生产者组名称，用于标识一组发送同类消息的生产者。
     */
    private final String producerGroup;

    /**
     * NameServer，用于在发送消息前查询 Broker 路由。
     */
    private final NameServer nameServer;

    /**
     * 创建生产者。
     *
     * @param producerGroup 生产者组名称。
     * @param nameServer NameServer 实例。
     */
    public Producer(String producerGroup, NameServer nameServer) {
        this.producerGroup = producerGroup;
        this.nameServer = nameServer;
    }

    /**
     * 发送消息。
     *
     * @param topic Topic 名称。
     * @param key 消息业务 key。
     * @param body 消息内容。
     */
    public void send(String topic, String key, String body) {
        Broker broker = nameServer.route(topic);
        Message message = new Message(topic, key, body);
        broker.appendMessage(message);
        System.out.println("ProducerGroup=" + producerGroup + " 发送消息：" + key);
    }
}

package com.example.rocketmqstudy.concepts;

import com.example.rocketmqstudy.concepts.broker.Broker;
import com.example.rocketmqstudy.concepts.consumer.Consumer;
import com.example.rocketmqstudy.concepts.nameserver.NameServer;
import com.example.rocketmqstudy.concepts.producer.Producer;

/**
 * RocketMQ 概念章节的程序入口，用于演示核心角色和消息流转。
 */
public final class RocketMqConceptDemo {

    /**
     * 示例使用的 Topic 名称，用于表示订单相关消息分类。
     */
    private static final String ORDER_TOPIC = "order-topic";

    /**
     * 工具类示例不需要被实例化。
     */
    private RocketMqConceptDemo() {
    }

    /**
     * 程序入口，演示 Producer、NameServer、Broker、Queue 和 ConsumerGroup 的关系。
     *
     * @param args 命令行参数，本示例不使用。
     */
    public static void main(String[] args) {
        NameServer nameServer = new NameServer();
        Broker broker = new Broker("broker-a");
        broker.createTopic(ORDER_TOPIC, 2);
        nameServer.registerTopic(ORDER_TOPIC, broker);

        Producer producer = new Producer("order-producer-group", nameServer);
        producer.send(ORDER_TOPIC, "KEY-1", "创建订单 1");
        producer.send(ORDER_TOPIC, "KEY-2", "创建订单 2");
        producer.send(ORDER_TOPIC, "KEY-3", "创建订单 3");
        producer.send(ORDER_TOPIC, "KEY-4", "创建订单 4");

        Consumer firstOrderConsumer = new Consumer("order-consumer-a", "order-consumer-group", nameServer, 0);
        Consumer secondOrderConsumer = new Consumer("order-consumer-b", "order-consumer-group", nameServer, 1);
        Consumer auditConsumer = new Consumer("audit-consumer-a", "audit-consumer-group", nameServer, 0);

        firstOrderConsumer.poll(ORDER_TOPIC);
        secondOrderConsumer.poll(ORDER_TOPIC);
        auditConsumer.poll(ORDER_TOPIC);
    }
}

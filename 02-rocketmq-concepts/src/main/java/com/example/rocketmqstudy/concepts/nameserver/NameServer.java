package com.example.rocketmqstudy.concepts.nameserver;

import com.example.rocketmqstudy.concepts.broker.Broker;

import java.util.HashMap;
import java.util.Map;

/**
 * NameServer，负责保存 Topic 到 Broker 的路由关系。
 */
public final class NameServer {

    /**
     * Topic 路由表，key 是 Topic 名称，value 是提供该 Topic 的 Broker。
     */
    private final Map<String, Broker> topicRouteTable = new HashMap<>();

    /**
     * 注册 Topic 路由。
     *
     * @param topic Topic 名称。
     * @param broker 提供该 Topic 的 Broker。
     */
    public void registerTopic(String topic, Broker broker) {
        topicRouteTable.put(topic, broker);
        System.out.println("NameServer 注册路由：topic=" + topic + " -> broker=" + broker.getName());
    }

    /**
     * 根据 Topic 查询 Broker。
     *
     * @param topic Topic 名称。
     * @return 保存该 Topic 消息的 Broker。
     */
    public Broker route(String topic) {
        Broker broker = topicRouteTable.get(topic);
        if (broker == null) {
            throw new IllegalArgumentException("NameServer 中没有找到 Topic 路由：" + topic);
        }
        return broker;
    }
}

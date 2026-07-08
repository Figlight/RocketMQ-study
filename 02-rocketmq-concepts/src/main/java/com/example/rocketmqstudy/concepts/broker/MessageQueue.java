package com.example.rocketmqstudy.concepts.broker;

import com.example.rocketmqstudy.concepts.model.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Queue，代表 Broker 中真实保存消息的队列。
 */
final class MessageQueue {

    /**
     * Queue 编号，用于区分同一个 Topic 下的多个队列。
     */
    private final int queueId;

    /**
     * 消息列表，用于按写入顺序保存消息。
     */
    private final List<Message> messages = new ArrayList<>();

    /**
     * 创建消息队列。
     *
     * @param queueId Queue 编号。
     */
    MessageQueue(int queueId) {
        this.queueId = queueId;
    }

    /**
     * 获取 Queue 编号。
     *
     * @return Queue 编号。
     */
    int getQueueId() {
        return queueId;
    }

    /**
     * 向队列追加消息。
     *
     * @param message 需要追加的消息。
     */
    void addMessage(Message message) {
        messages.add(message);
    }

    /**
     * 从指定 offset 开始读取消息。
     *
     * @param offset 起始消费位点。
     * @return 从 offset 开始的消息副本。
     */
    List<Message> getMessagesFrom(int offset) {
        if (offset >= messages.size()) {
            return List.of();
        }
        return new ArrayList<>(messages.subList(offset, messages.size()));
    }
}

package com.example.rocketmqstudy.concepts.broker;

import com.example.rocketmqstudy.concepts.model.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Broker，负责保存 Topic 队列、写入消息和维护消费进度。
 */
public final class Broker {

    /**
     * Broker 名称，用于在日志中区分不同 Broker。
     */
    private final String name;

    /**
     * Topic 到队列列表的映射，用于保存每个 Topic 下的多个 Queue。
     */
    private final Map<String, List<MessageQueue>> topicQueueTable = new HashMap<>();

    /**
     * Topic 的下一次写入队列下标，用于模拟 Producer 轮询写入多个 Queue。
     */
    private final Map<String, Integer> nextQueueIndexTable = new HashMap<>();

    /**
     * 消费进度表，key 由 ConsumerGroup、Topic 和 QueueId 组成，value 是下一次拉取 offset。
     */
    private final Map<String, Integer> consumerOffsetTable = new HashMap<>();

    /**
     * 创建 Broker。
     *
     * @param name Broker 名称。
     */
    public Broker(String name) {
        this.name = name;
    }

    /**
     * 获取 Broker 名称。
     *
     * @return Broker 名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 创建 Topic 和对应数量的 Queue。
     *
     * @param topic Topic 名称。
     * @param queueCount Queue 数量。
     */
    public void createTopic(String topic, int queueCount) {
        if (queueCount <= 0) {
            throw new IllegalArgumentException("Queue 数量必须大于 0");
        }
        List<MessageQueue> queues = new ArrayList<>();
        for (int queueId = 0; queueId < queueCount; queueId++) {
            queues.add(new MessageQueue(queueId));
        }
        topicQueueTable.put(topic, queues);
        nextQueueIndexTable.put(topic, 0);
        System.out.println("Broker 创建 Topic：" + topic + "，Queue 数量：" + queueCount);
    }

    /**
     * 写入消息到 Topic 下的某个 Queue。
     *
     * @param message 需要写入的消息。
     */
    public void appendMessage(Message message) {
        List<MessageQueue> queues = findQueues(message.getTopic());
        int queueIndex = nextQueueIndexTable.get(message.getTopic());
        MessageQueue targetQueue = queues.get(queueIndex);
        targetQueue.addMessage(message);
        nextQueueIndexTable.put(message.getTopic(), (queueIndex + 1) % queues.size());
        System.out.println("Broker 写入消息：key=" + message.getKey() + " -> queueId=" + targetQueue.getQueueId());
    }

    /**
     * 按 ConsumerGroup 和 Queue 拉取消息。
     *
     * @param consumerGroup 消费者组名称。
     * @param topic Topic 名称。
     * @param queueId Queue 编号。
     * @return 本次拉取到的消息列表。
     */
    public List<Message> pullMessages(String consumerGroup, String topic, int queueId) {
        MessageQueue queue = findQueue(topic, queueId);
        String offsetKey = buildOffsetKey(consumerGroup, topic, queueId);
        int offset = consumerOffsetTable.getOrDefault(offsetKey, 0);
        List<Message> messages = queue.getMessagesFrom(offset);
        consumerOffsetTable.put(offsetKey, offset + messages.size());
        return messages;
    }

    /**
     * 查找 Topic 下的全部 Queue。
     *
     * @param topic Topic 名称。
     * @return Topic 对应的 Queue 列表。
     */
    private List<MessageQueue> findQueues(String topic) {
        List<MessageQueue> queues = topicQueueTable.get(topic);
        if (queues == null) {
            throw new IllegalArgumentException("Broker 中没有这个 Topic：" + topic);
        }
        return queues;
    }

    /**
     * 查找 Topic 下的指定 Queue。
     *
     * @param topic Topic 名称。
     * @param queueId Queue 编号。
     * @return 指定 Queue。
     */
    private MessageQueue findQueue(String topic, int queueId) {
        return findQueues(topic).stream()
                .filter(queue -> queue.getQueueId() == queueId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Broker 中没有这个 Queue：" + queueId));
    }

    /**
     * 构建消费进度 key。
     *
     * @param consumerGroup 消费者组名称。
     * @param topic Topic 名称。
     * @param queueId Queue 编号。
     * @return 消费进度 key。
     */
    private String buildOffsetKey(String consumerGroup, String topic, int queueId) {
        return consumerGroup + "::" + topic + "::" + queueId;
    }
}

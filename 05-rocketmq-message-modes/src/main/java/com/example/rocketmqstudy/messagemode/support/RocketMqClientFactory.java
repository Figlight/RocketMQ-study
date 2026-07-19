package com.example.rocketmqstudy.messagemode.support;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

/**
 * RocketMQ 客户端工厂，集中处理 NameServer 地址等重复初始化逻辑。
 *
 * <p>统一创建生产者和消费者的好处：
 * <ul>
 *   <li>避免每个示例都重复写 setNamesrvAddr 等初始化代码</li>
 *   <li>以后要改配置时只需改这一个地方</li>
 *   <li>不同模式使用不同的客户端类，由工厂统一暴露</li>
 * </ul>
 *
 * <p>四种客户端类型：
 * <pre>
 *   ┌───────────────────────────────────────────────────────────┐
 *   │ 生产者（Producer）                                        │
 *   ├───────────────────────────────────────────────────────────┤
 *   │  DefaultMQProducer           →  普通消息（同步/异步/单向）│
 *   │  TransactionMQProducer       →  事务消息                  │
 *   └───────────────────────────────────────────────────────────┘
 *   ┌───────────────────────────────────────────────────────────┐
 *   │ 消费者（Consumer）                                        │
 *   ├───────────────────────────────────────────────────────────┤
 *   │  DefaultMQPushConsumer       →  Push 模式（被动接收）    │
 *   │  DefaultLitePullConsumer     →  Pull 模式（主动拉取）    │
 *   └───────────────────────────────────────────────────────────┘
 * </pre>
 */
public final class RocketMqClientFactory {

    /**
     * 工具类不允许被实例化。
     * 所有方法都是 static，通过类名直接调用即可。
     */
    private RocketMqClientFactory() {
    }

    /**
     * 创建普通消息生产者。
     *
     * <p>用于：同步发送、异步发送、单向发送、批量消息、延迟消息、顺序消息等。
     *
     * <p>注意：返回的生产者<strong>尚未调用 start()</strong>，调用方需要在使用前自己启动。
     *
     * @param config 运行配置，包含 NameServer 地址等信息。
     * @param producerGroup 生产者组名称，同一组的生产者共享负载均衡。
     * @return 尚未启动的普通消息生产者实例。
     */
    public static DefaultMQProducer createProducer(MessageModeConfig config, String producerGroup) {
        // 1. new 出生产者实例，指定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        // 2. 设置 NameServer 地址 —— 生产者需要通过它找到 Broker
        producer.setNamesrvAddr(config.getNamesrvAddr());
        // 3. 返回给调用方，由调用方决定什么时候 start() 和 shutdown()
        return producer;
    }

    /**
     * 创建 Push 模式消费者。
     *
     * <p>Push 模式特点：
     * <ul>
     *   <li>消费者注册一个 MessageListener，Broker 推消息过来时自动回调</li>
     *   <li>调用方不需要写循环，由 RocketMQ 内部线程驱动</li>
     *   <li>适合：实时消费、简单场景</li>
     * </ul>
     *
     * <p>返回的消费者<strong>尚未调用 start()</strong>，调用方需要：
     * <ol>
     *   <li>subscribe(topic, tag) — 订阅主题</li>
     *   <li>registerMessageListener(...) — 注册监听器</li>
     *   <li>start() — 启动消费者</li>
     * </ol>
     *
     * @param config 运行配置。
     * @param consumerGroup 消费者组名称，同一组消费者共同消费、负载均衡。
     * @return 尚未启动的 Push 模式消费者实例。
     */
    public static DefaultMQPushConsumer createPushConsumer(MessageModeConfig config, String consumerGroup) {
        // 创建 Push 消费者实例，设置消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        // 设置 NameServer 地址 —— 消费者通过它找到 Broker
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        return consumer;
    }

    /**
     * 创建 Lite Pull 模式消费者。
     *
     * <p>Pull 模式特点：
     * <ul>
     *   <li>调用方自己写 while 循环，主动调用 poll() 去拉消息</li>
     *   <li>消费节奏由调用方自己控制（Thread.sleep 间隔等）</li>
     *   <li>适合：批量处理、需要控速的场景</li>
     * </ul>
     *
     * <p>返回的消费者<strong>尚未调用 start()</strong>，调用方需要：
     * <ol>
     *   <li>subscribe(topic, tag) — 订阅主题</li>
     *   <li>setAutoCommit(true/false) — 设置是否自动提交消费位点</li>
     *   <li>start() — 启动消费者</li>
     *   <li>while(...) { consumer.poll(timeout) } — 自己循环拉消息</li>
     * </ol>
     *
     * @param config 运行配置。
     * @param consumerGroup 消费者组名称。
     * @return 尚未启动的 Pull 模式消费者实例。
     */
    public static DefaultLitePullConsumer createPullConsumer(MessageModeConfig config, String consumerGroup) {
        // 创建 Lite Pull 消费者实例，设置消费者组名
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(consumerGroup);
        // 设置 NameServer 地址
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        return consumer;
    }

    /**
     * 创建事务消息生产者。
     *
     * <p>事务消息流程：发送半消息 → 执行本地事务 → 提交/回滚 → Broker 决定是否投递
     *
     * @param config 运行配置。
     * @param producerGroup 生产者组名称。
     * @return 尚未启动的事务消息生产者实例。
     */
    public static TransactionMQProducer createTransactionProducer(MessageModeConfig config, String producerGroup) {
        // 创建事务消息生产者实例
        TransactionMQProducer producer = new TransactionMQProducer(producerGroup);
        // 设置 NameServer 地址
        producer.setNamesrvAddr(config.getNamesrvAddr());
        return producer;
    }
}
package com.example.rocketmqstudy.messagemode.config;

import java.util.Arrays;

/**
 * 消息场景枚举 —— 统一管理示例程序中各种消息类型的 Topic 和默认 Tag 订阅表达式。
 *
 * <p><strong>为什么用枚举而不是字符串常量？</strong>
 * <pre>
 *   场景          Topic 名称              默认 Tag 表达式
 *   ─────────────────────────────────────────────────────
 *   NORMAL        StudyNormalTopic         *
 *   DELAY         StudyDelayTopic          *
 *   ORDERED       StudyOrderedTopic        *
 *   BATCH         StudyBatchTopic          *
 *   TRANSACTION   StudyTransactionTopic    *
 *   FILTER        StudyFilterTopic         TagA || TagB
 *   ─────────────────────────────────────────────────────
 *
 *   每个场景都有自己的 Topic，原因：
 *   1. 消费者组的消费位点（offset）以 Topic 为维度存储
 *   2. 如果不同场景共用一个 Topic，消费进度会相互干扰
 *   3. 独立 Topic 便于在 RocketMQ 控制台观察和管理
 * </pre>
 */
public enum MessageScenario {

    /**
     * 普通消息场景 —— 最基础的消息发送和消费。
     *
     * <p>用途：演示同步/异步/单向发送、Push 消费、Pull 消费等所有基础功能。
     */
    NORMAL("normal", "rocketmq.topic.normal", "ROCKETMQ_TOPIC_NORMAL", "StudyNormalTopic", "*"),

    /**
     * 延迟消息场景 —— 消息发送后，延迟一段时间才能被消费。
     *
     * <p>用途：演示 producer.send(msg, delayLevel) 的使用。
     * 独立 Topic：避免普通消息和延迟消息的消费位点混淆。
     */
    DELAY("delay", "rocketmq.topic.delay", "ROCKETMQ_TOPIC_DELAY", "StudyDelayTopic", "*"),

    /**
     * 顺序消息场景 —— 同一业务键（Key）的消息发送到同一个队列，保证消费顺序。
     *
     * <p>用途：演示如何用 MessageQueueSelector 让消息落到指定队列，
     * 以及如何注册顺序监听器 MessageListenerOrderly。
     */
    ORDERED("ordered", "rocketmq.topic.ordered", "ROCKETMQ_TOPIC_ORDERED", "StudyOrderedTopic", "*"),

    /**
     * 批量消息场景 —— 一次发送多条消息，减少网络开销，提升吞吐量。
     *
     * <p>用途：演示 producer.send(List<Message>) 的使用。
     */
    BATCH("batch", "rocketmq.topic.batch", "ROCKETMQ_TOPIC_BATCH", "StudyBatchTopic", "*"),

    /**
     * 事务消息场景 —— 发送"半消息" → 执行本地事务 → 提交/回滚 → Broker 决定是否投递。
     *
     * <p>用途：演示事务消息的完整流程，包括本地事务执行和事务回查。
     */
    TRANSACTION("transaction", "rocketmq.topic.transaction", "ROCKETMQ_TOPIC_TRANSACTION",
            "StudyTransactionTopic", "*"),

    /**
     * 消息过滤场景 —— 同一 Topic 下用 Tag 区分消息类型，消费者只订阅特定 Tag。
     *
     * <p>用途：演示 Tag 订阅表达式（如 "TagA || TagB"），
     * 以及用 Message Key 进行业务定位查询。
     */
    FILTER("filter", "rocketmq.topic.filter", "ROCKETMQ_TOPIC_FILTER", "StudyFilterTopic", "TagA || TagB");

    /**
     * 命令行使用的场景名称 —— 小写字符串，如 "normal"、"delay"。
     *
     * <p>用法示例：java PushMessageConsumer normal
     */
    private final String argumentName;

    /**
     * 覆盖 Topic 时使用的 JVM 系统属性名称。
     *
     * <p>用法示例：java -Drocketmq.topic.normal=MyTopic BasicSendModeProducer
     */
    private final String topicPropertyName;

    /**
     * 覆盖 Topic 时使用的环境变量名称（全大写 + 下划线）。
     *
     * <p>用法示例：export ROCKETMQ_TOPIC_NORMAL=MyTopic
     */
    private final String topicEnvironmentName;

    /**
     * 场景默认使用的 Topic 名称 —— 没有配置覆盖时使用这个。
     *
     * <p>命名规则：Study + 场景名 + Topic，便于识别是教学示例使用的 Topic。
     */
    private final String defaultTopic;

    /**
     * 场景默认使用的消费者 Tag 订阅表达式。
     *
     * <p>常见值：
     * <ul>
     *   <li>"*" —— 订阅所有 Tag（大多数场景）</li>
     *   <li>"TagA || TagB" —— 只订阅 TagA 或 TagB（FILTER 场景）</li>
     *   <li>"TagA" —— 只订阅 TagA</li>
     * </ul>
     */
    private final String defaultTagExpression;

    /**
     * 创建消息场景定义 —— 枚举的构造方法，由编译器自动调用。
     *
     * @param argumentName 命令行场景名称（小写）。
     * @param topicPropertyName Topic 系统属性名称（-D 参数）。
     * @param topicEnvironmentName Topic 环境变量名称。
     * @param defaultTopic 默认 Topic 名称。
     * @param defaultTagExpression 默认 Tag 订阅表达式。
     */
    MessageScenario(String argumentName, String topicPropertyName, String topicEnvironmentName,
                    String defaultTopic, String defaultTagExpression) {
        this.argumentName = argumentName;
        this.topicPropertyName = topicPropertyName;
        this.topicEnvironmentName = topicEnvironmentName;
        this.defaultTopic = defaultTopic;
        this.defaultTagExpression = defaultTagExpression;
    }

    /**
     * 根据命令行参数解析消息场景。
     *
     * <p>使用 Java 8 Stream API：
     * <ol>
     *   <li>values() —— 获取所有枚举值（NORMAL, DELAY, ORDERED, BATCH, TRANSACTION, FILTER）</li>
     *   <li>Arrays.stream() —— 转成 Stream<MessageScenario></li>
     *   <li>filter() —— 过滤出 argumentName 与传入值匹配的项（忽略大小写）</li>
     *   <li>findFirst() —— 取第一个（按枚举定义顺序）</li>
     *   <li>orElseThrow() —— 找不到则抛异常（告知用户所有可选值）</li>
     * </ol>
     *
     * @param value 命令行传入的场景名称（如 "normal"、"delay"）。
     * @return 匹配的消息场景枚举值。
     * @throws IllegalArgumentException 当场景名称不受支持时抛出，附可选值列表。
     */
    public static MessageScenario fromArgument(String value) {
        return Arrays.stream(values())
                .filter(scenario -> scenario.argumentName.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "不支持的消息场景：" + value + "，可选值：normal、delay、ordered、batch、transaction、filter"));
    }

    /**
     * 获取命令行场景名称（如 "normal"）。
     *
     * @return 命令行场景名称。
     */
    public String getArgumentName() {
        return argumentName;
    }

    /**
     * 获取 Topic 系统属性名称（如 "rocketmq.topic.normal"）。
     *
     * @return Topic 系统属性名称。
     */
    public String getTopicPropertyName() {
        return topicPropertyName;
    }

    /**
     * 获取 Topic 环境变量名称（如 "ROCKETMQ_TOPIC_NORMAL"）。
     *
     * @return Topic 环境变量名称。
     */
    public String getTopicEnvironmentName() {
        return topicEnvironmentName;
    }

    /**
     * 获取默认 Topic 名称（如 "StudyNormalTopic"）。
     *
     * @return 默认 Topic 名称。
     */
    public String getDefaultTopic() {
        return defaultTopic;
    }

    /**
     * 获取默认 Tag 订阅表达式（如 "*" 或 "TagA || TagB"）。
     *
     * @return 默认 Tag 订阅表达式。
     */
    public String getDefaultTagExpression() {
        return defaultTagExpression;
    }
}
package com.example.rocketmqstudy.messagemode.config;

/**
 * 消息模式示例的运行配置 —— 集中管理 NameServer、Topic、分组名称等信息。
 *
 * <p><strong>配置读取优先级（从高到低）：</strong>
 * <pre>
 *   1. JVM 系统属性（-Drocketmq.namesrvAddr=...）
 *   2. 操作系统环境变量（ROCKETMQ_NAMESRV_ADDR=...）
 *   3. 代码中的默认值（DEFAULT_*）
 *
 *   为什么这样设计？
 *   - 默认值：让示例代码开箱即用，无需任何配置
 *   - 系统属性：开发时临时覆盖，快速测试
 *   - 环境变量：Docker/K8s 部署时通过环境注入
 *   - 三种方式覆盖同一字段，不修改代码就能切换环境
 * </pre>
 *
 * <p><strong>使用示例：</strong>
 * <pre>
 *   方式 1：改代码默认值 → 修改本文件中的 DEFAULT_* 常量
 *
 *   方式 2：JVM 启动参数
 *     java -Drocketmq.namesrvAddr=192.168.1.10:9876 \
 *          -Drocketmq.topic.normal=MyTopic \
 *          BasicSendModeProducer
 *
 *   方式 3：环境变量
 *     export ROCKETMQ_NAMESRV_ADDR=192.168.1.10:9876
 *     java BasicSendModeProducer
 * </pre>
 */
public final class MessageModeConfig {

    /**
     * 默认 NameServer 地址。
     *
     * <p>NameServer 是 RocketMQ 的"注册中心"，生产者/消费者通过它找到 Broker 的地址。
     * 默认值指向本机 9876 端口（RocketMQ 标准端口）。
     * 如果你的 NameServer 在其他机器，需要通过 -Drocketmq.namesrvAddr 覆盖。
     */
    private static final String DEFAULT_NAMESRV_ADDR = "127.0.0.1:9876";

    /**
     * 默认生产者组前缀。
     *
     * <p>完整组名 = 前缀 + 后缀（由调用方传入，如 "basic-sync"）。
     * 作用：同一组的生产者共享负载均衡和故障转移配置。
     */
    private static final String DEFAULT_PRODUCER_GROUP_PREFIX = "message-mode-producer";

    /**
     * 默认消费者组前缀。
     *
     * <p>完整组名 = 前缀 + 后缀（由调用方传入，如 "push-normal"）。
     * 作用：同一组的消费者共享消费位点（offset），组成集群消费。
     */
    private static final String DEFAULT_CONSUMER_GROUP_PREFIX = "message-mode-consumer";

    /**
     * NameServer 地址（最终使用的值，从配置读取而来）。
     */
    private final String namesrvAddr;

    /**
     * 生产者组名称前缀。
     */
    private final String producerGroupPrefix;

    /**
     * 消费者组名称前缀。
     */
    private final String consumerGroupPrefix;

    /**
     * 私有构造方法 —— 通过 MessageModeConfig.load() 工厂方法创建实例。
     *
     * <p>使用私有构造 + 静态工厂方法的好处：
     * <ul>
     *   <li>控制实例创建逻辑（只能通过 load() 创建，不能随便 new）</li>
     *   <li>创建过程可以做异常处理、缓存（虽然本例没缓存）</li>
     *   <li>类一旦创建就是不可变对象（immutable），字段全是 final</li>
     * </ul>
     *
     * @param namesrvAddr NameServer 地址。
     * @param producerGroupPrefix 生产者组前缀。
     * @param consumerGroupPrefix 消费者组前缀。
     */
    private MessageModeConfig(String namesrvAddr, String producerGroupPrefix, String consumerGroupPrefix) {
        this.namesrvAddr = namesrvAddr;
        this.producerGroupPrefix = producerGroupPrefix;
        this.consumerGroupPrefix = consumerGroupPrefix;
    }

    /**
     * 从 JVM 系统属性和环境变量加载配置。
     *
     * <p>这是创建配置对象的唯一入口。每次调用都会重新读取当前环境变量和系统属性。
     *
     * @return 加载完成的配置对象（不可变）。
     */
    public static MessageModeConfig load() {
        // 分别读取三个配置项，每个都走 "系统属性 → 环境变量 → 默认值" 的查找链
        return new MessageModeConfig(
                readConfig("rocketmq.namesrvAddr", "ROCKETMQ_NAMESRV_ADDR", DEFAULT_NAMESRV_ADDR),
                readConfig("rocketmq.producerGroupPrefix", "ROCKETMQ_PRODUCER_GROUP_PREFIX",
                        DEFAULT_PRODUCER_GROUP_PREFIX),
                readConfig("rocketmq.consumerGroupPrefix", "ROCKETMQ_CONSUMER_GROUP_PREFIX",
                        DEFAULT_CONSUMER_GROUP_PREFIX)
        );
    }

    /**
     * 获取 NameServer 地址。
     *
     * @return NameServer 地址（如 "127.0.0.1:9876"）。
     */
    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    /**
     * 获取指定场景实际使用的 Topic 名称。
     *
     * <p>Topic 也支持配置覆盖，例如：
     * <pre>
     *   java -Drocketmq.topic.normal=MyCustomTopic BasicSendModeProducer
     * </pre>
     *
     * @param scenario 消息场景（NORMAL/DELAY/ORDERED 等）。
     * @return 配置覆盖后或默认的 Topic 名称。
     */
    public String topicFor(MessageScenario scenario) {
        // 从场景定义中拿到对应的系统属性名、环境变量名、默认值，然后走通用读取逻辑
        return readConfig(scenario.getTopicPropertyName(), scenario.getTopicEnvironmentName(),
                scenario.getDefaultTopic());
    }

    /**
     * 获取指定场景实际使用的 Tag 订阅表达式。
     *
     * <p>Tag 表达式用于消费者过滤订阅，例如：
     * <pre>
     *   "*"              —— 订阅所有 Tag（默认）
     *   "TagA"           —— 只订阅 TagA
     *   "TagA || TagB"   —— 订阅 TagA 或 TagB
     *
     *   // 使用环境变量覆盖：
     *   java -Drocketmq.tagExpression="TagA" PushMessageConsumer filter
     * </pre>
     *
     * @param scenario 消息场景。
     * @return 配置覆盖后或默认的 Tag 订阅表达式。
     */
    public String tagExpressionFor(MessageScenario scenario) {
        return readConfig("rocketmq.tagExpression", "ROCKETMQ_TAG_EXPRESSION",
                scenario.getDefaultTagExpression());
    }

    /**
     * 生成指定示例使用的生产者组名称。
     *
     * <p>完整组名 = 前缀 + "-" + 后缀
     *
     * @param suffix 组名后缀，区分不同发送模式（如 "basic-sync"、"batch"）。
     * @return 完整生产者组名称（如 "message-mode-producer-basic-sync"）。
     */
    public String producerGroup(String suffix) {
        return producerGroupPrefix + "-" + suffix;
    }

    /**
     * 生成指定示例使用的消费者组名称。
     *
     * <p>完整组名 = 前缀 + "-" + 后缀
     *
     * <p><strong>特别注意：</strong>不同场景用不同的消费者组名很重要！
     * 因为同一组的消费者共享消费位点（offset），如果两个不同场景用同一个组名，
     * 消费位点会相互干扰。
     *
     * @param suffix 组名后缀，区分不同消费场景（如 "pull-normal"、"ordered"）。
     * @return 完整消费者组名称（如 "message-mode-consumer-pull-normal"）。
     */
    public String consumerGroup(String suffix) {
        return consumerGroupPrefix + "-" + suffix;
    }

    /**
     * 按照"系统属性 → 环境变量 → 默认值"的优先级读取单个配置项。
     *
     * <p>查找逻辑（找到第一个非空值就返回）：
     * <ol>
     *   <li>System.getProperty(propertyName) —— 如 -Drocketmq.namesrvAddr=...</li>
     *   <li>System.getenv(environmentName) —— 如 export ROCKETMQ_NAMESRV_ADDR=...</li>
     *   <li>defaultValue —— 代码里的默认值（兜底）</li>
     * </ol>
     *
     * <p>isBlank() 检查：排除空字符串和全空白的情况。
     *
     * @param propertyName JVM 系统属性名称（如 "rocketmq.namesrvAddr"）。
     * @param environmentName 环境变量名称（如 "ROCKETMQ_NAMESRV_ADDR"）。
     * @param defaultValue 默认值，兜底用。
     * @return 最终使用的配置值。
     */
    private static String readConfig(String propertyName, String environmentName, String defaultValue) {
        // 优先看系统属性（-D 参数）
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }

        // 再看环境变量（适合在 Docker/K8s 部署环境使用）
        String environmentValue = System.getenv(environmentName);
        if (environmentValue != null && !environmentValue.isBlank()) {
            return environmentValue;
        }

        // 都没有，返回默认值（代码中写死的默认配置）
        return defaultValue;
    }
}
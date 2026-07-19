package com.example.rocketmqstudy.messagemode.consumer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.ConsumerLifecycle;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Push 模式并发消费者 —— Broker 主动把消息"推"给消费者。
 *
 * <p>Push 模式的核心思想：
 * <pre>
 *   【你】            注册一个监听器给消费者，说"有消息了调用这个方法"
 *   【RocketMQ】      后台线程不停去 Broker 拉消息（名字叫 Push 实际底层也是 Pull）
 *   【RocketMQ】      拉到消息后回调你的 consumeMessage() 方法
 *   【你】            在 consumeMessage() 里处理业务
 *
 *   流程图：
 *   main() → create consumer → subscribe → register listener → start → awaitShutdown
 *                                                                             ↑
 *                                     RocketMQ 后台线程调用 consumeMessage() ←┘
 * </pre>
 *
 * <p>支持消费的消息场景：普通消息、延迟消息、批量消息、事务消息、过滤消息
 * （顺序消息使用专门的 OrderedMessageConsumer）
 */
public final class PushMessageConsumer {

    /**
     * 工具型入口类不允许被实例化。
     * 所有逻辑都在 main 方法里，直接运行即可。
     */
    private PushMessageConsumer() {
    }

    /**
     * 程序入口，根据场景参数订阅对应 Topic 和 Tag 表达式。
     *
     * <p>命令行用法示例：
     * <pre>
     *   java PushMessageConsumer              →  默认消费 normal 场景
     *   java PushMessageConsumer delay        →  消费延迟消息
     *   java PushMessageConsumer transaction  →  消费事务消息
     *   java PushMessageConsumer filter       →  消费过滤消息（TagA || TagB）
     * </pre>
     *
     * @param args 第一个参数为 normal、delay、batch、transaction 或 filter，默认 normal。
     * @throws Exception 当消费者启动或主线程等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        // ──────────────── 第 1 步：解析场景，加载配置 ────────────────
        // 从命令行参数解析消费场景（normal/delay/batch 等）
        MessageScenario scenario = parseScenario(args);
        // 加载运行配置（NameServer 地址、Topic 名称等）
        MessageModeConfig config = MessageModeConfig.load();
        // 根据场景获取对应 Topic
        String topic = config.topicFor(scenario);
        // 根据场景获取 Tag 订阅表达式（如 "*" 表示订阅所有 Tag）
        String tagExpression = config.tagExpressionFor(scenario);

        // ──────────────── 第 2 步：创建消费者 ────────────────
        // 通过工厂创建 Push 消费者，消费者组名 = 前缀 + "push-" + 场景名
        // 不同场景用不同组名，消费进度互不影响
        DefaultMQPushConsumer consumer = RocketMqClientFactory.createPushConsumer(
                config, config.consumerGroup("push-" + scenario.getArgumentName()));

        // ──────────────── 第 3 步：配置消费策略 ────────────────
        // 从最早的消息开始消费（新组第一次启动时的策略）
        // 如果换成 CONSUME_FROM_LAST_OFFSET，则从最新位置开始，跳过历史消息
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅 Topic 和 Tag 表达式：只接收匹配的消息
        consumer.subscribe(topic, tagExpression);

        // ──────────────── 第 4 步：注册消息监听器（Push 模式特有） ────────────────
        // 注册一个并发监听器：有消息时 RocketMQ 会调用 consumeMessage()
        // 这是 Push 模式与 Pull 模式最大的区别 —— Pull 模式不需要注册监听器，自己循环 poll
        consumer.registerMessageListener(new PrintingConcurrentMessageListener());

        // ──────────────── 第 5 步：启动消费者 ────────────────
        // start() 之后，RocketMQ 内部线程开始工作：
        //   1. 连接 NameServer 获取 Broker 地址
        //   2. 向 Broker 注册自己（加入消费者组）
        //   3. 启动后台拉消息线程
        consumer.start();

        // ──────────────── 第 6 步：主线程等待退出信号 ────────────────
        // 消费者是长驻程序，不能让 main 结束
        // awaitShutdown 会阻塞主线程，直到收到 Ctrl+C 信号
        // 收到信号时自动调用 consumer.shutdown() 优雅关闭
        ConsumerLifecycle.awaitShutdown(
                "Push 消费者启动成功：scenario=" + scenario.getArgumentName()
                        + "，topic=" + topic + "，tagExpression=" + tagExpression,
                consumer::shutdown  // 关闭钩子：JVM 退出时执行的动作
        );
    }

    /**
     * 解析并校验 Push 消费者支持的消息场景。
     *
     * <p>注意：顺序消息（ordered）有专门的消费者 OrderedMessageConsumer，
     * 因为它需要注册顺序监听器 MessageListenerOrderly 而不是并发监听器。
     *
     * @param args 命令行参数。
     * @return 可使用并发监听器消费的消息场景。
     */
    private static MessageScenario parseScenario(String[] args) {
        // 如果没传参数，默认使用 normal 场景
        MessageScenario scenario = args.length == 0
                ? MessageScenario.NORMAL
                : MessageScenario.fromArgument(args[0]);
        // 顺序消息需要使用专门的 OrderedMessageConsumer，这里不允许
        if (scenario == MessageScenario.ORDERED) {
            throw new IllegalArgumentException("顺序消息请使用 OrderedMessageConsumer");
        }
        return scenario;
    }

    /**
     * 打印消息并返回消费成功状态的并发消费监听器。
     *
     * <p><strong>这是 Push 模式的核心：</strong>
     * <ul>
     *   <li>你<strong>不要主动调用</strong> consumeMessage() 方法</li>
     *   <li>由 RocketMQ 后台线程在有新消息时<strong>自动回调</strong>这个方法</li>
     *   <li>并发监听器（Concurrently）：多个线程同时消费，不保证顺序</li>
     *   <li>顺序监听器（Orderly）：同一队列的消息串行消费，保证顺序</li>
     * </ul>
     */
    private static final class PrintingConcurrentMessageListener implements MessageListenerConcurrently {

        /**
         * 逐条打印当前批次消息并确认消费成功。
         *
         * <p>返回值含义：
         * <ul>
         *   <li>CONSUME_SUCCESS —— 消费成功，告诉 Broker 可以推进消费位点</li>
         *   <li>RECONSUME_LATER —— 消费失败，稍后重试（消息会被重新投递给消费者）</li>
         * </ul>
         *
         * @param messages 本次投递的消息列表（可能是 1 条或多条，由 Broker 批量推送）
         * @param context 并发消费上下文（可以在这里设置重试延迟等高级参数）
         * @return 固定返回消费成功（示例代码不处理失败场景）
         */
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                                                        ConsumeConcurrentlyContext context) {
            // 打印本次收到的消息数量
            System.out.println("本次回调消息数=" + messages.size());
            // 遍历并打印每条消息（topic/tag/key/msgId/body 等信息）
            // forEach 是 Java 8 的方法引用写法，等价于 for (MessageExt msg : messages) { printMessage(msg) }
            messages.forEach(MessageSupport::printMessage);
            // 确认消费成功 —— Broker 收到这个状态后会推进消费位点
            // 如果这里抛异常或返回 RECONSUME_LATER，消息会被重新投递
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }
}
package com.example.rocketmqstudy.messagemode.consumer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Lite Pull 模式消费者 —— 消费者主动去 Broker"拉"消息。
 *
 * <p>Pull 模式与 Push 模式最大的区别：
 * <pre>
 *   ┌──────────────┬───────────────────────────────────────────────────┐
 *   │              │          Pull 模式（本示例）                       │
 *   ├──────────────┼───────────────────────────────────────────────────┤
 *   │  谁主动？     │  消费者主动调用 poll()                            │
 *   │  消费入口     │  自己写 while 循环 + consumer.poll()              │
 *   │  消费速率     │  自己控制（Thread.sleep 间隔）                    │
 *   │  线程模型     │  单线程（你可以自己改成多线程）                    │
 *   │  适合场景     │  批量处理、需要控速保护下游系统                    │
 *   └──────────────┴───────────────────────────────────────────────────┘
 *
 *   本示例的流程图：
 *   main() → create consumer → subscribe → start → while(true) { poll → sleep }
 *                                                      ↑
 *                                             这是 Pull 模式的核心：
 *                                             你自己调用 poll() 拿消息
 * </pre>
 */
public final class PullMessageConsumer {

    /**
     * 单次拉取最长等待毫秒数。
     *
     * <p>含义：调用 consumer.poll(POLL_TIMEOUT_MILLIS) 时，
     * 如果没有消息，最多等待这么久就返回（空列表）。
     * 这样做的好处是不会一直阻塞，可以在没有消息时做些别的事情。
     */
    private static final long POLL_TIMEOUT_MILLIS = 1_000L;

    /**
     * 两次主动拉取之间的演示间隔毫秒数。
     *
     * <p>含义：每轮循环结束后 Thread.sleep 这么久，避免空转拉爆 CPU。
     * 实际项目中可以根据业务需要调整这个值，比如：
     * <ul>
     *   <li>追求实时 → 设为 0 或不 sleep</li>
     *   <li>批量处理保护下游 → 设为较大值（如 5~10 秒）</li>
     * </ul>
     */
    private static final long POLL_INTERVAL_MILLIS = 500L;

    /**
     * 工具型入口类不允许被实例化。
     */
    private PullMessageConsumer() {
    }

    /**
     * 程序入口，主动循环拉取普通消息 Topic。
     *
     * <p>与 Push 模式对比：这里<strong>没有</strong> registerMessageListener，
     * 而是自己写 while 循环，主动调用 consumer.poll() 去拿消息。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当消费者启动、拉取或线程等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        // ──────────────── 第 1 步：加载配置 ────────────────
        MessageModeConfig config = MessageModeConfig.load();
        // Pull 模式示例消费普通消息场景
        MessageScenario scenario = MessageScenario.NORMAL;
        // 根据场景获取 Topic 名称
        String topic = config.topicFor(scenario);

        // ──────────────── 第 2 步：创建 Pull 消费者 ────────────────
        // 通过工厂创建 Lite Pull 消费者，组名 = 前缀 + "pull-normal"
        // 注意：Pull 和 Push 用不同的消费者组名，避免消费位点冲突
        DefaultLitePullConsumer consumer = RocketMqClientFactory.createPullConsumer(
                config, config.consumerGroup("pull-normal"));

        // ──────────────── 第 3 步：配置消费策略（Pull 模式特有） ────────────────
        // setAutoCommit(true) = 自动提交消费位点
        //   → 你 poll 拿到消息后，consumer 自动把位点上报给 Broker
        // setAutoCommit(false) = 手动提交
        //   → 你需要在处理完消息后显式调用 consumer.commitSync()
        consumer.setAutoCommit(true);

        // 订阅 Topic，"*" 表示接收所有 Tag 的消息
        consumer.subscribe(topic, "*");

        // ──────────────── 第 4 步：注册优雅关闭钩子 ────────────────
        // JVM 退出时（比如 Ctrl+C）自动关闭消费者，确保：
        //   1. 消费位点正确提交
        //   2. 网络连接正常关闭
        //   3. 后台线程优雅退出
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown, "rocketmq-pull-shutdown"));

        // ──────────────── 第 5 步：启动消费者 ────────────────
        // start() 之后 consumer 才会真正连接 NameServer 和 Broker
        consumer.start();

        // ──────────────── 第 6 步：主循环 —— Pull 模式核心 ────────────────
        System.out.println("Pull 消费者启动成功：topic=" + topic);
        System.out.println("客户端将主动 poll；按 Ctrl+C 停止消费者");

        // 条件：当前线程没有被中断（被中断时说明程序要退出了）
        while (!Thread.currentThread().isInterrupted()) {
            // ★★★ 主动调用 poll() 拉取消息 ★★★
            // 这是 Pull 模式与 Push 模式最本质的区别：
            //   Push → 等 RocketMQ 回调你
            //   Pull → 你主动问 "有消息吗？给我来一批"
            // 参数 POLL_TIMEOUT_MILLIS = 最长等待时间，超时没消息就返回空列表
            List<MessageExt> messages = consumer.poll(POLL_TIMEOUT_MILLIS);

            if (messages.isEmpty()) {
                // 没有拉到消息，打印提示（实际项目中这一行可以去掉，减少日志）
                System.out.println("本轮没有拉取到消息");
            } else {
                // 拉到了消息，遍历打印每条消息
                // 实际项目中这里应该处理业务逻辑（存数据库、调用下游接口等）
                messages.forEach(MessageSupport::printMessage);
            }

            // 控制拉取间隔，避免对 Broker 和下游系统造成过大压力
            // 如果追求极致实时性，可以去掉这行；如果是离线批量处理，可以调大
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
    }
}
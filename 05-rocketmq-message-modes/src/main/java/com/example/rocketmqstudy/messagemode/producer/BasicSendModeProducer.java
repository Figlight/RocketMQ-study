package com.example.rocketmqstudy.messagemode.producer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基础发送模式生产者 —— 演示三种消息发送方式：同步、异步、单向。
 *
 * <p>三种发送模式的对比：
 * <pre>
 *   ┌──────────┬──────────────────────┬──────────────────────┬─────────────────────────┐
 *   │          │ 同步发送 (sync)      │ 异步发送 (async)     │ 单向发送 (oneway)       │
 *   ├──────────┼──────────────────────┼──────────────────────┼─────────────────────────┤
 *   │ 发送方式  │ producer.send(msg)  │ producer.send(msg,   │ producer.sendOneway(msg)│
 *   │          │                      │    callback)         │                         │
 *   │ 主线程   │ 阻塞等待 Broker ACK   │ 立即返回，结果在回调中 │ 立即返回，无结果        │
 *   │ 可靠性   │ ★★★ 最高（有 SendResult）│ ★★ 较高（失败有回调） │ ★ 最低（可能丢失）      │
 *   │ 吞吐量   │ 低（等待一个来回）     │ 高（不阻塞）          │ 最高（不等待）           │
 *   │ 适用场景 │ 重要消息（订单、支付） │ 批量消息、通知         │ 日志、监控（丢几条没关系）│
 *   └──────────┴──────────────────────┴──────────────────────┴─────────────────────────┘
 *
 *   命令行用法：
 *   java BasicSendModeProducer              → 默认 sync（同步）
 *   java BasicSendModeProducer async        → 异步发送
 *   java BasicSendModeProducer oneway       → 单向发送
 * </pre>
 */
public final class BasicSendModeProducer {

    /**
     * 等待异步发送回调的最长秒数。
     *
     * <p>含义：如果 10 秒内没收到 Broker 的回调（成功 or 失败），
     * 就认为超时并抛出异常，避免程序无限等待。
     */
    private static final int ASYNC_CALLBACK_TIMEOUT_SECONDS = 10;

    /**
     * 工具型入口类不允许被实例化。
     */
    private BasicSendModeProducer() {
    }

    /**
     * 程序入口，根据第一个命令行参数选择 sync、async 或 oneway 发送模式。
     *
     * @param args 第一个参数为发送模式，不传则默认使用 sync。
     * @throws Exception 当客户端启动或消息发送失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        // ──────────────── 第 1 步：解析发送模式 ────────────────
        // 从命令行参数解析出发送模式（sync/async/oneway），不传默认为 sync
        SendMode sendMode = SendMode.fromArguments(args);

        // ──────────────── 第 2 步：加载配置并创建生产者 ────────────────
        MessageModeConfig config = MessageModeConfig.load();
        // 普通消息场景的 Topic
        String topic = config.topicFor(MessageScenario.NORMAL);
        // 创建生产者：组名 = 前缀 + "basic-" + 模式名
        // 不同模式用不同组名，便于在 RocketMQ 控制台区分
        DefaultMQProducer producer = RocketMqClientFactory.createProducer(
                config, config.producerGroup("basic-" + sendMode.argumentName));

        try {
            // ──────────────── 第 3 步：启动生产者 ────────────────
            // start() 后，生产者才会真正连接 NameServer 和 Broker
            producer.start();

            // ──────────────── 第 4 步：构建消息 ────────────────
            // 通过工具类统一构建消息（UTF-8 编码）
            Message message = MessageSupport.buildMessage(
                    topic,
                    "Basic" + sendMode.displayName,          // Tag：Basic同步/Basic异步/Basic单向
                    "basic-" + sendMode.argumentName + "-001", // Key：业务主键
                    sendMode.displayName + "消息：Hello RocketMQ" // Body：正文
            );

            // ──────────────── 第 5 步：按选定模式发送 ────────────────
            sendByMode(producer, message, sendMode);
        } finally {
            // ──────────────── 第 6 步：关闭生产者（无论成功失败都要执行） ────────────────
            // finally 保证：即使 try 块里抛出异常，shutdown() 也一定会被调用
            // shutdown() 会断开连接、清理线程池，否则可能导致资源泄漏
            producer.shutdown();
        }
    }

    /**
     * 按指定模式发送消息（简单的模式分派方法）。
     *
     * @param producer 已启动的生产者。
     * @param message 待发送消息。
     * @param sendMode 发送模式（SYNC / ASYNC / ONEWAY）。
     * @throws Exception 当发送失败或异步回调超时时抛出。
     */
    private static void sendByMode(DefaultMQProducer producer, Message message, SendMode sendMode) throws Exception {
        // 使用 Java 14+ 的 switch 表达式（带箭头语法）
        // 如果你的 IDE 或 JDK 版本不支持，会提示错误
        switch (sendMode) {
            case SYNC -> sendSynchronously(producer, message);
            case ASYNC -> sendAsynchronously(producer, message);
            case ONEWAY -> sendOneway(producer, message);
            default -> throw new IllegalStateException("未处理的发送模式：" + sendMode);
        }
    }

    /**
     * 同步发送消息并打印 Broker 确认结果。
     *
     * <p><strong>同步发送的特点：</strong>
     * <ul>
     *   <li>调用 producer.send(msg) 后，主线程会<strong>阻塞</strong>等待，直到 Broker 返回 SendResult</li>
     *   <li>SendResult 包含：发送状态（SEND_OK）、msgId、消息落在哪一个 MessageQueue 等信息</li>
     *   <li>可靠性最高：有返回值就能明确知道消息是否发送成功</li>
     *   <li>吞吐量最低：每条消息都要等一个网络来回（RTT）</li>
     * </ul>
     *
     * <p><strong>适用场景：</strong>重要消息（如订单创建、支付通知），不能容忍丢失。
     *
     * @param producer 已启动的生产者。
     * @param message 待发送消息。
     * @throws Exception 当同步发送失败时抛出（如 Broker 不可达、超时等）。
     */
    private static void sendSynchronously(DefaultMQProducer producer, Message message) throws Exception {
        // ★ 同步发送：send() 阻塞，直到 Broker 返回 SendResult
        SendResult sendResult = producer.send(message);
        // 打印 Broker 返回的结果，例如：
        // SendResult [sendStatus=SEND_OK, msgId=C0A80068..., offsetMsgId=C0A80068...]
        System.out.println("同步发送完成，Broker 返回：" + sendResult);
    }

    /**
     * 异步发送消息并等待成功或失败回调。
     *
     * <p><strong>异步发送的特点：</strong>
     * <ul>
     *   <li>调用 producer.send(msg, callback) 后，主线程<strong>立即返回</strong>，不会阻塞</li>
     *   <li>Broker 确认结果通过<strong>后台线程回调</strong> onSuccess() 或 onException()</li>
     *   <li>吞吐量高：主线程可以继续处理其他工作，不用等网络</li>
     *   <li>需要注意：回调是在 RocketMQ 内部线程执行的，不要在回调里写耗时长的逻辑</li>
     * </ul>
     *
     * <p><strong>本方法额外做的事：</strong>
     * <pre>
     *   为了让示例程序"看到"发送结果，本方法用 CountDownLatch 做了同步等待：
     *   1. CountDownLatch(1) —— 一把门闩，await() 会阻塞直到 countDown() 被调用
     *   2. AtomicReference —— 线程安全地在回调和主线程之间传递异常
     *   3. 不管回调成功还是失败，都会调用 countDown() 唤醒主线程
     *   4. 主线程拿到结果后再决定是正常退出还是抛异常
     *
     *   时间线：
     *   主线程 ── send(msg, callback) ──→ 立即返回 → print → latch.await() ──┐
     *              后台线程 ← 发送消息到 Broker ← ... ← Broker ACK ── onSuccess → countDown() ─┘
     *                                                                 或 onException
     * </pre>
     *
     * @param producer 已启动的生产者。
     * @param message 待发送消息。
     * @throws Exception 当发送调用失败、回调失败或等待超时时抛出。
     */
    private static void sendAsynchronously(DefaultMQProducer producer, Message message) throws Exception {
        // ──────────────── 用于主线程和回调线程同步的工具 ────────────────

        // CountDownLatch(1)：计数为 1 的门闩
        // - 主线程调用 latch.await() → 阻塞等待
        // - 回调线程调用 latch.countDown() → 计数-1（变为 0），唤醒主线程
        // 用途：让主线程"等"回调结果
        CountDownLatch callbackLatch = new CountDownLatch(1);

        // AtomicReference<Throwable>：线程安全的异常持有者
        // - 回调线程（onException）：把异常存进来
        // - 主线程：被唤醒后检查里面是否有异常，有就重新抛出
        // 为什么不用普通变量？因为跨线程读写需要内存可见性保证
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();

        // ──────────────── 第 1 步：异步发送消息（主线程立即返回） ────────────────
        producer.send(message, new SendCallback() {

            /**
             * 发送成功回调（由 RocketMQ 后台线程调用，不是主线程）。
             *
             * @param sendResult Broker 返回的发送结果。
             */
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步发送成功，Broker 返回：" + sendResult);
                // 告诉主线程："我成功了，你可以继续了"
                callbackLatch.countDown();
            }

            /**
             * 发送失败回调（由 RocketMQ 后台线程调用）。
             *
             * @param throwable 失败原因。
             */
            @Override
            public void onException(Throwable throwable) {
                // 先把异常存起来（线程安全的方式）
                callbackFailure.set(throwable);
                // 同样告诉主线程："结束了，你检查结果吧"
                callbackLatch.countDown();
            }
        });

        // ──────────────── 第 2 步：send() 已经返回，主线程可以继续干活 ────────────────
        // 注意：到这一行时，消息可能还没到 Broker，onSuccess/onException 还没被调用！
        System.out.println("异步发送调用已返回，主线程可以继续处理其他工作");

        // ──────────────── 第 3 步：等待回调结果（超时保护） ────────────────
        // 阻塞主线程，最多等 10 秒
        // 返回值 = true 表示在超时前被唤醒（countDown 被调用了）
        // 返回值 = false 表示超时了，可能消息丢了或 Broker 挂了
        boolean callbackCompleted = callbackLatch.await(ASYNC_CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!callbackCompleted) {
            // 超时：10 秒内没收到回调，可能 Broker 没响应或网络问题
            throw new IllegalStateException("等待异步发送回调超时");
        }
        if (callbackFailure.get() != null) {
            // 回调里记录了异常：发送失败，把异常重新抛出给调用方
            throw new IllegalStateException("异步发送失败", callbackFailure.get());
        }
        // 到这里说明：回调成功收到且没有异常 → 发送成功
    }

    /**
     * 单向发送消息，不等待 Broker 返回任何结果。
     *
     * <p><strong>单向发送的特点：</strong>
     * <ul>
     *   <li>调用 producer.sendOneway(msg) 后，主线程<strong>立即返回</strong></li>
     *   <li>没有 SendResult，也没有回调，<strong>完全不知道消息是否发送成功</strong></li>
     *   <li>吞吐量最高：只负责"把消息发出去"，不等任何回复</li>
     *   <li>可靠性最低：如果此刻 Broker 挂了或网络不通，消息可能<strong>静默丢失</strong></li>
     * </ul>
     *
     * <p><strong>适用场景：</strong>日志、监控数据、心跳信号等 ——
     * 丢几条没关系，重要的是高吞吐量。
     *
     * @param producer 已启动的生产者。
     * @param message 待发送消息。
     * @throws Exception 当单向发送调用失败时抛出（注意：Broker 端失败不会抛）。
     */
    private static void sendOneway(DefaultMQProducer producer, Message message) throws Exception {
        // ★ 单向发送：发出去就不管了，没有返回值，没有回调
        producer.sendOneway(message);
        System.out.println("单向发送调用已完成；该模式没有 SendResult，存在消息丢失风险");
    }

    /**
     * 基础消息发送模式 —— sync/async/oneway 的枚举定义。
     *
     * <p>用枚举而不是用字符串常量的好处：
     * <ul>
     *   <li>类型安全：编译器检查，不会出现拼写错误 "synk" 这种情况</li>
     *   <li>附带元数据：每个枚举值可以带 argumentName、displayName 等多个字段</li>
     *   <li>switch 语法清晰</li>
     * </ul>
     */
    private enum SendMode {

        /**
         * 同步发送（默认）：调用 send() 阻塞，等 Broker 返回 SendResult。
         */
        SYNC("sync", "同步"),

        /**
         * 异步发送：send(msg, callback) 立即返回，结果通过回调获取。
         */
        ASYNC("async", "异步"),

        /**
         * 单向发送：sendOneway() 立即返回，不管结果，可能丢消息。
         */
        ONEWAY("oneway", "单向");

        /**
         * 命令行参数名称（小写）—— 用户传的是这个字符串。
         */
        private final String argumentName;

        /**
         * 中文显示名称（用于打印提示）。
         */
        private final String displayName;

        /**
         * 构造发送模式定义。
         *
         * @param argumentName 命令行参数名称（如 "sync"）。
         * @param displayName 中文显示名称（如 "同步"）。
         */
        SendMode(String argumentName, String displayName) {
            this.argumentName = argumentName;
            this.displayName = displayName;
        }

        /**
         * 从命令行参数解析发送模式。
         *
         * <p>逻辑：
         * <ol>
         *   <li>如果 args 为空，默认用 SYNC</li>
         *   <li>否则把 args[0] 转小写，然后和三个枚举值匹配</li>
         *   <li>匹配不到抛出 IllegalArgumentException</li>
         * </ol>
         *
         * @param args 命令行参数。
         * @return 解析出的发送模式。
         */
        private static SendMode fromArguments(String[] args) {
            // 如果没传参数，默认 sync；否则取第一个参数并转小写（便于大小写不敏感）
            String value = args.length == 0 ? SYNC.argumentName : args[0].toLowerCase(Locale.ROOT);
            // 遍历所有枚举值，找到匹配的
            for (SendMode sendMode : values()) {
                if (sendMode.argumentName.equals(value)) {
                    return sendMode;
                }
            }
            // 没找到匹配，抛出异常（提示用户可选值）
            throw new IllegalArgumentException("不支持的发送模式：" + value + "，可选值：sync、async、oneway");
        }
    }
}
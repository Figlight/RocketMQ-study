package com.example.rocketmqstudy.messagemode.support;

import java.util.concurrent.CountDownLatch;

/**
 * 长驻消费者生命周期工具 —— 让消费者程序优雅地运行和退出。
 *
 * <p>解决的问题：
 * <pre>
 *   1. main() 方法执行完后，程序就结束了，消费者不能继续跑
 *   2. 直接 Ctrl+C 杀进程，RocketMQ 客户端可能来不及提交消费位点
 *   3. 需要在退出时做一些清理工作（关闭连接、提交位点等）
 *
 *   解决方案：
 *   ┌─────────────────────────────────────────────────────┐
 *   │ 1. 注册 ShutdownHook —— JVM 退出前自动执行清理动作 │
 *   │ 2. CountDownLatch.await() —— 阻塞主线程不让它结束  │
 *   │ 3. 按 Ctrl+C → JVM 收到信号 → 执行 ShutdownHook → 优雅退出 │
 *   └─────────────────────────────────────────────────────┘
 * </pre>
 */
public final class ConsumerLifecycle {

    /**
     * 工具类不允许被实例化。
     */
    private ConsumerLifecycle() {
    }

    /**
     * 注册关闭钩子并阻塞主线程，直到进程被终止。
     *
     * <p>内部原理：
     * <ol>
     *   <li><b>addShutdownHook</b>：告诉 JVM —— "你要退出前帮我执行这个动作"（比如关闭 consumer）</li>
     *   <li><b>打印启动消息</b>：告诉用户程序已经正常启动</li>
     *   <li><b>new CountDownLatch(1).await()</b>：创建一个计数为 1 的门闩并等待它被打开。
     *       因为永远没人调用 countDown()，主线程就一直卡在这里，直到 JVM 退出。</li>
     * </ol>
     *
     * <p>为什么不用 while(true) + Thread.sleep()？
     * <ul>
     *   <li>while(true) 会占用 CPU 时间片（虽然 sleep 了但依然在"自旋"）</li>
     *   <li>CountDownLatch.await() 是<strong>真正的阻塞</strong>，不消耗 CPU</li>
     *   <li>响应中断信号（Ctrl+C）时能正确唤醒并退出</li>
     * </ul>
     *
     * @param startupMessage 消费者启动成功后输出的提示（告诉用户启动成功了）。
     * @param shutdownAction JVM 退出前执行的关闭动作（比如 consumer::shutdown）。
     * @throws InterruptedException 当主线程等待期间被中断时抛出（正常的 Ctrl+C 不会抛这个）。
     */
    public static void awaitShutdown(String startupMessage, Runnable shutdownAction) throws InterruptedException {
        // 注册 JVM 关闭钩子：JVM 要退出前，会在一个新线程里执行 shutdownAction
        // 典型的 shutdownAction 是 consumer::shutdown，作用：
        //   1. 提交当前消费位点到 Broker
        //   2. 断开与 Broker 和 NameServer 的连接
        //   3. 清理内部线程池
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownAction, "rocketmq-client-shutdown"));

        // 打印启动成功消息
        System.out.println(startupMessage);
        System.out.println("按 Ctrl+C 停止消费者");

        // ★ 阻塞主线程 ★
        // 创建一个计数为 1 的 CountDownLatch，然后 await()
        // 因为本方法里没人调用 countDown()，所以它会一直等在这里
        // 直到：
        //   a) JVM 被外部信号（Ctrl+C、kill 命令）终止 → ShutdownHook 执行 → JVM 退出
        //   b) 当前线程被 interrupt() → 抛出 InterruptedException
        new CountDownLatch(1).await();
    }
}
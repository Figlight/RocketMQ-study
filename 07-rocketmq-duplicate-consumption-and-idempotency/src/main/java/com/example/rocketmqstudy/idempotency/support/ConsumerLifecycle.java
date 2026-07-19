package com.example.rocketmqstudy.idempotency.support;

import java.util.concurrent.CountDownLatch;

/**
 * 长驻消费者生命周期工具。
 */
public final class ConsumerLifecycle {

    /** 工具类不允许实例化。 */
    private ConsumerLifecycle() {
    }

    /**
     * 注册关闭钩子并阻塞主线程。
     *
     * @param startupMessage 启动成功提示。
     * @param shutdownAction 关闭客户端的动作。
     * @throws InterruptedException 主线程被中断时抛出。
     */
    public static void awaitShutdown(String startupMessage, Runnable shutdownAction)
            throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownAction, "rocketmq-client-shutdown"));
        System.out.println(startupMessage);
        System.out.println("按 Ctrl+C 停止消费者");
        new CountDownLatch(1).await();
    }
}

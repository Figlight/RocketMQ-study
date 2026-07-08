package com.example.rocketmqstudy.introduction;

import com.example.rocketmqstudy.introduction.consumer.CouponConsumer;
import com.example.rocketmqstudy.introduction.consumer.InventoryConsumer;
import com.example.rocketmqstudy.introduction.consumer.PointConsumer;
import com.example.rocketmqstudy.introduction.mq.InMemoryMessageQueue;
import com.example.rocketmqstudy.introduction.service.OrderSystem;

import java.util.concurrent.TimeUnit;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * RocketMQ 简介章节的程序入口，用于演示 MQ 的异步、解耦和削峰思想。
 */
public final class IntroductionDemo {

    /**
     * 工具类示例不需要被实例化。
     */
    private IntroductionDemo() {
    }

    /**
     * 程序入口，先启动内存 MQ，再连续发送订单事件观察异步消费效果。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws InterruptedException 当主线程等待消费者处理消息时被中断。
     */
    public static void main(String[] args) throws InterruptedException {
        try (InMemoryMessageQueue messageQueue = new InMemoryMessageQueue()) {
            messageQueue.registerConsumer(new InventoryConsumer());
            messageQueue.registerConsumer(new CouponConsumer());
            messageQueue.registerConsumer(new PointConsumer());
            messageQueue.start();

            OrderSystem orderSystem = new OrderSystem(messageQueue);

            log("开始模拟 5 个订单突然进入系统");
            for (int index = 1; index <= 5; index++) {
                String orderId = "ORDER-" + index;
                orderSystem.placeOrder(orderId, "USER-100" + index, 100 + index);
            }

            log("订单系统已经把消息全部交给 MQ，主流程可以继续处理其他请求");
            TimeUnit.SECONDS.sleep(4);
        }
    }
}

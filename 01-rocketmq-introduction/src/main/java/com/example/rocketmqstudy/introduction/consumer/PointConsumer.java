package com.example.rocketmqstudy.introduction.consumer;

import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;

import java.util.concurrent.TimeUnit;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * 积分消费者，模拟订单创建后增加用户积分。
 */
public final class PointConsumer implements OrderEventConsumer {

    /**
     * 消费订单事件并模拟增加积分。
     *
     * @param event 订单创建事件。
     * @throws InterruptedException 当模拟耗时被中断。
     */
    @Override
    public void consume(OrderPlacedEvent event) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(200);
        log("积分系统增加积分：" + event.getAmount() + "，订单：" + event.getOrderId());
    }
}

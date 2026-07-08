package com.example.rocketmqstudy.introduction.consumer;

import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;

import java.util.concurrent.TimeUnit;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * 优惠券消费者，模拟订单创建后发送优惠券。
 */
public final class CouponConsumer implements OrderEventConsumer {

    /**
     * 消费订单事件并模拟发送优惠券。
     *
     * @param event 订单创建事件。
     * @throws InterruptedException 当模拟耗时被中断。
     */
    @Override
    public void consume(OrderPlacedEvent event) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(300);
        log("优惠券系统给用户 " + event.getUserId() + " 发送复购券");
    }
}

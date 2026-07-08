package com.example.rocketmqstudy.introduction.consumer;

import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;

/**
 * 订单事件消费者接口，代表一个订阅订单事件的业务系统。
 */
public interface OrderEventConsumer {

    /**
     * 消费订单事件。
     *
     * @param event 订单创建事件。
     * @throws InterruptedException 当模拟耗时业务被中断。
     */
    void consume(OrderPlacedEvent event) throws InterruptedException;
}

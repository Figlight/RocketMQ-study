package com.example.rocketmqstudy.introduction.consumer;

import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;

import java.util.concurrent.TimeUnit;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * 库存消费者，模拟订单创建后扣减库存。
 */
public final class InventoryConsumer implements OrderEventConsumer {

    /**
     * 消费订单事件并模拟扣减库存。
     *
     * @param event 订单创建事件。
     * @throws InterruptedException 当模拟耗时被中断。
     */
    @Override
    public void consume(OrderPlacedEvent event) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(250);
        log("库存系统完成扣减：" + event.getOrderId());
    }
}

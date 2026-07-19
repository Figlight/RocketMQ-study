package com.example.rocketmqstudy.idempotency.service;

import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模拟订单支付后发放积分的业务服务。
 */
public final class OrderRewardService {

    /** 当前进程累计执行的积分发放次数。 */
    private final AtomicInteger executionCount = new AtomicInteger();

    /** 当前进程累计发放的积分数量。 */
    private final AtomicInteger totalRewardPoints = new AtomicInteger();

    /**
     * 执行有副作用的积分发放业务并打印累计结果。
     *
     * @param event 订单支付完成事件。
     */
    public void grantRewardPoints(OrderPaidEvent event) {
        int currentExecutionCount = executionCount.incrementAndGet();
        int currentTotalPoints = totalRewardPoints.addAndGet(event.getRewardPoints());
        System.out.printf("发放积分：orderNumber=%s, userId=%s, points=%d, 执行次数=%d, 累计积分=%d%n",
                event.getOrderNumber(), event.getUserId(), event.getRewardPoints(),
                currentExecutionCount, currentTotalPoints);
    }
}

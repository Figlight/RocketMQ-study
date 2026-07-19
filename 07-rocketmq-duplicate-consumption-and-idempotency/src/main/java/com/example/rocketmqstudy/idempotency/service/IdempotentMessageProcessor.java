package com.example.rocketmqstudy.idempotency.service;

import com.example.rocketmqstudy.idempotency.model.ClaimResult;
import com.example.rocketmqstudy.idempotency.model.OrderPaidEvent;
import com.example.rocketmqstudy.idempotency.model.ProcessingResult;
import com.example.rocketmqstudy.idempotency.repository.IdempotencyRecordRepository;

/**
 * 幂等消息处理编排器，协调幂等检查、业务处理和结果记录。
 */
public final class IdempotentMessageProcessor {

    /** 幂等处理记录仓储。 */
    private final IdempotencyRecordRepository recordRepository;

    /** 订单积分业务服务。 */
    private final OrderRewardService rewardService;

    /**
     * 创建幂等消息处理编排器。
     *
     * @param recordRepository 幂等处理记录仓储。
     * @param rewardService 订单积分业务服务。
     */
    public IdempotentMessageProcessor(IdempotencyRecordRepository recordRepository,
                                      OrderRewardService rewardService) {
        this.recordRepository = recordRepository;
        this.rewardService = rewardService;
    }

    /**
     * 按“抢占处理权、执行业务、记录成功”的顺序处理事件。
     *
     * @param event 订单支付完成事件。
     * @return 本次处理结果。
     */
    public ProcessingResult process(OrderPaidEvent event) {
        String businessKey = event.businessKey();
        ClaimResult claimResult = recordRepository.tryStart(businessKey);
        if (claimResult == ClaimResult.ALREADY_SUCCEEDED) {
            System.out.println("检测到已成功的业务 Key，跳过重复业务：" + businessKey);
            return ProcessingResult.DUPLICATE_SKIPPED;
        }
        if (claimResult == ClaimResult.ALREADY_PROCESSING) {
            System.out.println("相同业务 Key 正在处理中，稍后重试：" + businessKey);
            return ProcessingResult.RETRY_LATER;
        }

        try {
            rewardService.grantRewardPoints(event);
            recordRepository.markSucceeded(businessKey);
            System.out.println("幂等处理成功并记录结果：" + businessKey);
            return ProcessingResult.EXECUTED;
        } catch (RuntimeException exception) {
            recordRepository.release(businessKey);
            throw exception;
        }
    }
}

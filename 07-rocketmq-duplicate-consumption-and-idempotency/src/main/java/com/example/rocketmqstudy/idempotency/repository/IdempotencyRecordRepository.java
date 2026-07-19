package com.example.rocketmqstudy.idempotency.repository;

import com.example.rocketmqstudy.idempotency.model.ClaimResult;

/**
 * 幂等处理记录仓储，负责处理权抢占和处理结果持久化语义。
 */
public interface IdempotencyRecordRepository {

    /**
     * 原子地尝试为业务 Key 创建“处理中”记录。
     *
     * @param businessKey 稳定业务唯一 Key。
     * @return 处理权抢占结果。
     */
    ClaimResult tryStart(String businessKey);

    /**
     * 将业务 Key 标记为处理成功。
     *
     * @param businessKey 稳定业务唯一 Key。
     */
    void markSucceeded(String businessKey);

    /**
     * 业务失败时释放“处理中”记录，使 RocketMQ 重试可以再次处理。
     *
     * @param businessKey 稳定业务唯一 Key。
     */
    void release(String businessKey);
}

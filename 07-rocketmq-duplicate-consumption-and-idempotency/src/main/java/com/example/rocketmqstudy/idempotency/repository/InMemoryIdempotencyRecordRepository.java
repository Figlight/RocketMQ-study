package com.example.rocketmqstudy.idempotency.repository;

import com.example.rocketmqstudy.idempotency.model.ClaimResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于进程内存的幂等记录仓储，仅用于演示原子抢占，不适合生产环境。
 */
public final class InMemoryIdempotencyRecordRepository implements IdempotencyRecordRepository {

    /** 内存中的业务 Key 与处理状态映射。 */
    private final ConcurrentMap<String, RecordStatus> records = new ConcurrentHashMap<>();

    /**
     * 使用 putIfAbsent 原子抢占处理权，避免并发的“先查后写”竞态。
     *
     * @param businessKey 稳定业务唯一 Key。
     * @return 处理权抢占结果。
     */
    @Override
    public ClaimResult tryStart(String businessKey) {
        RecordStatus existingStatus = records.putIfAbsent(businessKey, RecordStatus.PROCESSING);
        if (existingStatus == null) {
            return ClaimResult.ACQUIRED;
        }
        return existingStatus == RecordStatus.SUCCEEDED
                ? ClaimResult.ALREADY_SUCCEEDED
                : ClaimResult.ALREADY_PROCESSING;
    }

    /**
     * 将已经存在的处理记录更新为成功。
     *
     * @param businessKey 稳定业务唯一 Key。
     */
    @Override
    public void markSucceeded(String businessKey) {
        if (!records.replace(businessKey, RecordStatus.PROCESSING, RecordStatus.SUCCEEDED)) {
            throw new IllegalStateException("无法将不存在或状态异常的幂等记录标记成功：" + businessKey);
        }
    }

    /**
     * 仅删除仍处于处理中的记录，不覆盖其他线程已经写入的成功状态。
     *
     * @param businessKey 稳定业务唯一 Key。
     */
    @Override
    public void release(String businessKey) {
        records.remove(businessKey, RecordStatus.PROCESSING);
    }

    /**
     * 内存幂等记录状态。
     */
    private enum RecordStatus {

        /** 业务正在处理。 */
        PROCESSING,

        /** 业务已经处理成功。 */
        SUCCEEDED
    }
}

package com.example.rocketmqstudy.idempotency.model;

/**
 * 幂等消息处理结果。
 */
public enum ProcessingResult {

    /** 本次调用实际执行并完成了业务。 */
    EXECUTED,

    /** 业务此前已经成功，本次重复投递被跳过。 */
    DUPLICATE_SKIPPED,

    /** 另一个线程正在处理同一业务 Key，本次应稍后重试。 */
    RETRY_LATER
}

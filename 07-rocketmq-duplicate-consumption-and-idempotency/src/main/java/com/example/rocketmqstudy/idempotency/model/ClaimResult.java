package com.example.rocketmqstudy.idempotency.model;

/**
 * 幂等处理权抢占结果。
 */
public enum ClaimResult {

    /** 当前调用成功取得处理权，可以执行业务。 */
    ACQUIRED,

    /** 相同业务 Key 已处理成功，不应再次执行业务。 */
    ALREADY_SUCCEEDED,

    /** 相同业务 Key 正由其他线程处理，应稍后重试。 */
    ALREADY_PROCESSING
}

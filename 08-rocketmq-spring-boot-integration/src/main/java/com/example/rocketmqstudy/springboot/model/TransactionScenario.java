package com.example.rocketmqstudy.springboot.model;

import java.util.Locale;

/**
 * 事务消息本地事务执行场景。
 */
public enum TransactionScenario {

    /** 本地事务立即成功并提交半消息。 */
    COMMIT,

    /** 本地事务失败并回滚半消息。 */
    ROLLBACK,

    /** 首次状态未知，等待 Broker 回查后提交。 */
    UNKNOWN_THEN_COMMIT;

    /**
     * 从 REST 参数解析事务场景。
     *
     * @param value 场景字符串。
     * @return 对应事务场景。
     */
    public static TransactionScenario from(String value) {
        if (value == null || value.isBlank()) {
            return COMMIT;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "事务场景只支持 commit、rollback 或 unknown_then_commit：" + value, exception);
        }
    }
}

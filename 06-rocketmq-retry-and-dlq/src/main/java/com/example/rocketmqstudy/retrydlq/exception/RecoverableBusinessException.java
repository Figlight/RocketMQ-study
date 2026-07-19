package com.example.rocketmqstudy.retrydlq.exception;

/**
 * 可恢复业务异常，表示稍后重试存在成功可能。
 */
public final class RecoverableBusinessException extends Exception {

    /**
     * 创建可恢复业务异常。
     *
     * @param message 异常说明。
     */
    public RecoverableBusinessException(String message) {
        super(message);
    }
}

package com.example.rocketmqstudy.retrydlq.exception;

/**
 * 不可恢复业务异常，表示重复执行无法改变结果。
 */
public final class UnrecoverableBusinessException extends Exception {

    /**
     * 创建不可恢复业务异常。
     *
     * @param message 异常说明。
     */
    public UnrecoverableBusinessException(String message) {
        super(message);
    }
}

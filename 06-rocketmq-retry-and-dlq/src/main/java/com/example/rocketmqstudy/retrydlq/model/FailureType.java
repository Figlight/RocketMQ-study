package com.example.rocketmqstudy.retrydlq.model;

import java.util.Locale;

/**
 * 消费失败类型，用于决定重试还是直接进入死信队列。
 */
public enum FailureType {

    /** 可恢复异常，稍后重试可能成功。 */
    RECOVERABLE("recoverable"),

    /** 不可恢复异常，重复执行也不会成功。 */
    UNRECOVERABLE("unrecoverable");

    /** 命令行参数值。 */
    private final String argumentName;

    /**
     * 创建失败类型。
     *
     * @param argumentName 命令行参数值。
     */
    FailureType(String argumentName) {
        this.argumentName = argumentName;
    }

    /**
     * 从命令行参数解析失败类型。
     *
     * @param args 命令行参数。
     * @return 失败类型；未传参数时默认为可恢复异常。
     */
    public static FailureType fromArguments(String[] args) {
        String value = args.length == 0 ? RECOVERABLE.argumentName : args[0].toLowerCase(Locale.ROOT);
        for (FailureType type : values()) {
            if (type.argumentName.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("失败类型只支持 recoverable 或 unrecoverable：" + value);
    }

    /** @return 命令行参数值。 */
    public String getArgumentName() { return argumentName; }
}

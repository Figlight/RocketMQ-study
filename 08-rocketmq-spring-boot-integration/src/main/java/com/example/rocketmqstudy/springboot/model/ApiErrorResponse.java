package com.example.rocketmqstudy.springboot.model;

/**
 * REST 接口统一错误响应。
 */
public class ApiErrorResponse {

    /** 可直接展示给学习者的错误说明。 */
    private final String message;

    /**
     * 创建错误响应。
     *
     * @param message 错误说明。
     */
    public ApiErrorResponse(String message) {
        this.message = message;
    }

    /** @return 错误说明。 */
    public String getMessage() {
        return message;
    }
}

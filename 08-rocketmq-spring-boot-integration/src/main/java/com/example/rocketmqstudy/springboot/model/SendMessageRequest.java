package com.example.rocketmqstudy.springboot.model;

import javax.validation.constraints.NotBlank;

/**
 * REST 接口接收的字符串消息请求。
 */
public class SendMessageRequest {

    /** 消息正文。 */
    @NotBlank(message = "消息正文不能为空")
    private String message;

    /** 创建空请求对象，供 JSON 反序列化使用。 */
    public SendMessageRequest() {
    }

    /**
     * 创建字符串消息请求。
     *
     * @param message 消息正文。
     */
    public SendMessageRequest(String message) {
        this.message = message;
    }

    /** @return 消息正文。 */
    public String getMessage() {
        return message;
    }

    /** @param message 消息正文。 */
    public void setMessage(String message) {
        this.message = message;
    }
}

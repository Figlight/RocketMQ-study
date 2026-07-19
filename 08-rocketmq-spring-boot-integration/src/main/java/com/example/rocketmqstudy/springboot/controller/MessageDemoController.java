package com.example.rocketmqstudy.springboot.controller;

import com.example.rocketmqstudy.springboot.model.OrderMessage;
import com.example.rocketmqstudy.springboot.model.SendMessageRequest;
import com.example.rocketmqstudy.springboot.model.TransactionScenario;
import com.example.rocketmqstudy.springboot.producer.RocketMqMessageProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * 通过 REST 请求触发各类 RocketMQ Spring Boot 发送场景。
 */
@Validated
@RestController
@RequestMapping("/api/messages")
public class MessageDemoController {

    /** RocketMQ 消息发送服务。 */
    private final RocketMqMessageProducer messageProducer;

    /**
     * 创建消息演示控制器。
     *
     * @param messageProducer RocketMQ 消息发送服务。
     */
    public MessageDemoController(RocketMqMessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    /**
     * 同步发送字符串消息。
     *
     * @param request 字符串消息请求。
     * @return 发送结果摘要。
     */
    @PostMapping("/sync")
    public String sendSynchronously(@Valid @RequestBody SendMessageRequest request) {
        SendResult result = messageProducer.sendSynchronously(request.getMessage());
        return formatSendResult("同步消息", result);
    }

    /**
     * 异步发送字符串消息。
     *
     * @param request 字符串消息请求。
     * @return 请求受理说明。
     */
    @PostMapping("/async")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String sendAsynchronously(@Valid @RequestBody SendMessageRequest request) {
        messageProducer.sendAsynchronously(request.getMessage());
        return "异步发送请求已提交，请观察回调日志";
    }

    /**
     * 单向发送字符串消息。
     *
     * @param request 字符串消息请求。
     * @return 请求受理说明。
     */
    @PostMapping("/one-way")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String sendOneWay(@Valid @RequestBody SendMessageRequest request) {
        messageProducer.sendOneWay(request.getMessage());
        return "单向消息已交给客户端发送，不代表 Broker 已确认保存";
    }

    /**
     * 发送固定延迟等级消息。
     *
     * @param delayLevel RocketMQ 4.x 延迟等级。
     * @param request 字符串消息请求。
     * @return 发送结果摘要。
     */
    @PostMapping("/delay")
    public String sendDelayed(
            @RequestParam @Min(1) @Max(18) int delayLevel,
            @Valid @RequestBody SendMessageRequest request) {
        SendResult result = messageProducer.sendDelayed(request.getMessage(), delayLevel);
        return formatSendResult("延迟消息", result);
    }

    /**
     * 发送 JSON 订单对象消息。
     *
     * @param orderMessage 订单消息。
     * @return 发送结果摘要。
     */
    @PostMapping("/orders")
    public String sendOrder(@Valid @RequestBody OrderMessage orderMessage) {
        SendResult result = messageProducer.sendOrder(orderMessage);
        return formatSendResult("订单对象消息", result);
    }

    /**
     * 发送同一订单的有序状态变化消息。
     *
     * @param orderNumber 订单号。
     * @param userId 用户编号。
     * @return 发送完成说明。
     */
    @PostMapping("/orders/lifecycle")
    public String sendOrderLifecycle(
            @RequestParam @NotBlank String orderNumber,
            @RequestParam @NotBlank String userId) {
        messageProducer.sendOrderLifecycle(orderNumber, userId);
        return "订单状态消息已按 CREATED -> PAID -> SHIPPED -> RECEIVED 顺序发送";
    }

    /**
     * 发送带 Tag 的消息。
     *
     * @param tag 消息 Tag；监听器只接收 java 或 spring。
     * @param request 字符串消息请求。
     * @return 发送结果摘要。
     */
    @PostMapping("/tagged")
    public String sendTagged(@RequestParam @NotBlank String tag,
                             @Valid @RequestBody SendMessageRequest request) {
        SendResult result = messageProducer.sendTagged(tag, request.getMessage());
        return formatSendResult("Tag 消息", result);
    }

    /**
     * 发送事务消息。
     *
     * @param scenarioValue commit、rollback 或 unknown_then_commit。
     * @param request 字符串消息请求。
     * @return 事务发送结果摘要。
     */
    @PostMapping("/transaction")
    public String sendTransaction(
            @RequestParam(name = "scenario", defaultValue = "commit") String scenarioValue,
            @Valid @RequestBody SendMessageRequest request) {
        TransactionScenario scenario = TransactionScenario.from(scenarioValue);
        TransactionSendResult result = messageProducer.sendTransaction(request.getMessage(), scenario);
        return String.format("事务消息发送完成：sendStatus=%s, localTransactionState=%s, msgId=%s",
                result.getSendStatus(), result.getLocalTransactionState(), result.getMsgId());
    }

    /**
     * 连续发送消费模式演示消息。
     *
     * @param count 消息数量。
     * @return 发送完成说明。
     */
    @PostMapping("/consumer-models")
    public String sendConsumerModelMessages(
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int count) {
        messageProducer.sendConsumerModelMessages(count);
        return "已发送 " + count + " 条消费模式演示消息";
    }

    /**
     * 格式化普通发送结果。
     *
     * @param messageType 消息类型。
     * @param result RocketMQ 发送结果。
     * @return 发送结果摘要。
     */
    private String formatSendResult(String messageType, SendResult result) {
        return String.format("%s发送完成：sendStatus=%s, msgId=%s",
                messageType, result.getSendStatus(), result.getMsgId());
    }
}

package com.example.rocketmqstudy.springboot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;

/**
 * 学习示例使用的 Topic 与消费者组集中配置。
 */
@Validated
@ConfigurationProperties(prefix = "study.rocketmq")
public class StudyRocketMqProperties {

    /** 简单字符串消息 Topic。 */
    @NotBlank
    private String simpleTopic;

    /** 订单对象消息 Topic。 */
    @NotBlank
    private String orderTopic;

    /** 顺序订单消息 Topic。 */
    @NotBlank
    private String orderlyTopic;

    /** Tag 过滤消息 Topic。 */
    @NotBlank
    private String tagTopic;

    /** 事务消息 Topic。 */
    @NotBlank
    private String transactionTopic;

    /** 集群与广播消费模式共用的 Topic。 */
    @NotBlank
    private String modelTopic;

    /** 简单消息消费者组。 */
    @NotBlank
    private String simpleConsumerGroup;

    /** 对象消息消费者组。 */
    @NotBlank
    private String orderConsumerGroup;

    /** 顺序消息消费者组。 */
    @NotBlank
    private String orderlyConsumerGroup;

    /** Tag 过滤消费者组。 */
    @NotBlank
    private String tagConsumerGroup;

    /** 事务消息消费者组。 */
    @NotBlank
    private String transactionConsumerGroup;

    /** 集群消费模式消费者组。 */
    @NotBlank
    private String clusteringConsumerGroup;

    /** 广播消费模式消费者组。 */
    @NotBlank
    private String broadcastingConsumerGroup;

    /** @return 简单字符串消息 Topic。 */
    public String getSimpleTopic() {
        return simpleTopic;
    }

    /** @param simpleTopic 简单字符串消息 Topic。 */
    public void setSimpleTopic(String simpleTopic) {
        this.simpleTopic = simpleTopic;
    }

    /** @return 订单对象消息 Topic。 */
    public String getOrderTopic() {
        return orderTopic;
    }

    /** @param orderTopic 订单对象消息 Topic。 */
    public void setOrderTopic(String orderTopic) {
        this.orderTopic = orderTopic;
    }

    /** @return 顺序订单消息 Topic。 */
    public String getOrderlyTopic() {
        return orderlyTopic;
    }

    /** @param orderlyTopic 顺序订单消息 Topic。 */
    public void setOrderlyTopic(String orderlyTopic) {
        this.orderlyTopic = orderlyTopic;
    }

    /** @return Tag 过滤消息 Topic。 */
    public String getTagTopic() {
        return tagTopic;
    }

    /** @param tagTopic Tag 过滤消息 Topic。 */
    public void setTagTopic(String tagTopic) {
        this.tagTopic = tagTopic;
    }

    /** @return 事务消息 Topic。 */
    public String getTransactionTopic() {
        return transactionTopic;
    }

    /** @param transactionTopic 事务消息 Topic。 */
    public void setTransactionTopic(String transactionTopic) {
        this.transactionTopic = transactionTopic;
    }

    /** @return 消费模式演示 Topic。 */
    public String getModelTopic() {
        return modelTopic;
    }

    /** @param modelTopic 消费模式演示 Topic。 */
    public void setModelTopic(String modelTopic) {
        this.modelTopic = modelTopic;
    }

    /** @return 简单消息消费者组。 */
    public String getSimpleConsumerGroup() {
        return simpleConsumerGroup;
    }

    /** @param simpleConsumerGroup 简单消息消费者组。 */
    public void setSimpleConsumerGroup(String simpleConsumerGroup) {
        this.simpleConsumerGroup = simpleConsumerGroup;
    }

    /** @return 对象消息消费者组。 */
    public String getOrderConsumerGroup() {
        return orderConsumerGroup;
    }

    /** @param orderConsumerGroup 对象消息消费者组。 */
    public void setOrderConsumerGroup(String orderConsumerGroup) {
        this.orderConsumerGroup = orderConsumerGroup;
    }

    /** @return 顺序消息消费者组。 */
    public String getOrderlyConsumerGroup() {
        return orderlyConsumerGroup;
    }

    /** @param orderlyConsumerGroup 顺序消息消费者组。 */
    public void setOrderlyConsumerGroup(String orderlyConsumerGroup) {
        this.orderlyConsumerGroup = orderlyConsumerGroup;
    }

    /** @return Tag 过滤消费者组。 */
    public String getTagConsumerGroup() {
        return tagConsumerGroup;
    }

    /** @param tagConsumerGroup Tag 过滤消费者组。 */
    public void setTagConsumerGroup(String tagConsumerGroup) {
        this.tagConsumerGroup = tagConsumerGroup;
    }

    /** @return 事务消息消费者组。 */
    public String getTransactionConsumerGroup() {
        return transactionConsumerGroup;
    }

    /** @param transactionConsumerGroup 事务消息消费者组。 */
    public void setTransactionConsumerGroup(String transactionConsumerGroup) {
        this.transactionConsumerGroup = transactionConsumerGroup;
    }

    /** @return 集群消费模式消费者组。 */
    public String getClusteringConsumerGroup() {
        return clusteringConsumerGroup;
    }

    /** @param clusteringConsumerGroup 集群消费模式消费者组。 */
    public void setClusteringConsumerGroup(String clusteringConsumerGroup) {
        this.clusteringConsumerGroup = clusteringConsumerGroup;
    }

    /** @return 广播消费模式消费者组。 */
    public String getBroadcastingConsumerGroup() {
        return broadcastingConsumerGroup;
    }

    /** @param broadcastingConsumerGroup 广播消费模式消费者组。 */
    public void setBroadcastingConsumerGroup(String broadcastingConsumerGroup) {
        this.broadcastingConsumerGroup = broadcastingConsumerGroup;
    }
}

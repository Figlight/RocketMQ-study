package com.example.rocketmqstudy.messagemode.consumer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.ConsumerLifecycle;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 顺序消息消费者 —— 保证同一队列内的消息按发送顺序消费。
 *
 * <p><strong>顺序消费的核心原理：</strong>
 * <pre>
 *   【生产者】  发送消息时用同一 key（如 orderId），消息落到同一个 MessageQueue
 *   【Broker】  保证同一队列内的消息按发送顺序保存
 *   【消费者】  注册 MessageListenerOrderly 监听器，RocketMQ 对每个队列加锁
 *   【消费者】  同一时间只有一个线程消费该队列的消息，从而保证顺序
 *
 *   对比并发监听器 vs 顺序监听器：
 *   ┌───────────────────┬─────────────────────────────────┬─────────────────────────┐
 *   │                   │ MessageListenerConcurrently     │ MessageListenerOrderly  │
 *   ├───────────────────┼─────────────────────────────────┼─────────────────────────┤
 *   │ 线程模型           │ 多线程并发消费，互不影响          │ 单队列单线程，全局有序   │
 *   │ 吞吐量             │ 高（多线程并行）                 │ 较低（同一队列串行）     │
 *   │ 消息顺序           │ 不保证（不同消息不同线程）        │ 同一队列内严格按顺序     │
 *   │ 适用场景           │ 普通业务、日志、通知              │ 订单状态流转、支付流水   │
 *   └───────────────────┴─────────────────────────────────┴─────────────────────────┘
 * </pre>
 */
public final class OrderedMessageConsumer {

    /**
     * 工具型入口类不允许被实例化。
     */
    private OrderedMessageConsumer() {
    }

    /**
     * 程序入口，订阅顺序消息 Topic 并注册顺序监听器。
     *
     * <p>注意：虽然底层用的是 DefaultMQPushConsumer（Push 模式），
     * 但因为注册的是 MessageListenerOrderly 而不是 MessageListenerConcurrently，
     * 所以消费行为是"顺序"的而不是"并发"的。
     *
     * @param args 命令行参数，本示例不使用。
     * @throws Exception 当消费者启动或主线程等待失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        // 加载配置
        MessageModeConfig config = MessageModeConfig.load();
        // 获取顺序消息场景对应的 Topic
        String topic = config.topicFor(MessageScenario.ORDERED);
        // 创建 Push 消费者，组名 = 前缀 + "ordered"
        DefaultMQPushConsumer consumer = RocketMqClientFactory.createPushConsumer(
                config, config.consumerGroup("ordered"));

        // 从最早的消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅顺序消息 Topic，"*" 表示订阅所有 Tag
        consumer.subscribe(topic, "*");

        // ★★★ 注册顺序监听器（这是与并发消费唯一的代码区别） ★★★
        // RocketMQ 会：
        //   1. 为每个 MessageQueue 分配一个锁
        //   2. 同一时间只有一个消费者线程能持有某个队列的锁
        //   3. 获得锁的线程按队列顺序依次消费消息
        consumer.registerMessageListener(new PrintingOrderlyMessageListener());

        // 启动消费者
        consumer.start();

        // 主线程阻塞等待 Ctrl+C 退出信号
        ConsumerLifecycle.awaitShutdown(
                "顺序消费者启动成功：topic=" + topic,
                consumer::shutdown
        );
    }

    /**
     * 按队列顺序打印消息并确认成功的顺序消费监听器。
     *
     * <p>这个监听器的特点：
     * <ul>
     *   <li>实现的是 MessageListenerOrderly（顺序），而不是 MessageListenerConcurrently（并发）</li>
     *   <li>RocketMQ 会对每个 MessageQueue 加分布式锁，保证同一时刻只有一个消费者实例在消费</li>
     *   <li>同一消费者实例内，每个队列也只有一个线程在消费，从而保证队列内消息的顺序</li>
     * </ul>
     */
    private static final class PrintingOrderlyMessageListener implements MessageListenerOrderly {

        /**
         * 按 RocketMQ 提供的队列顺序处理当前批次消息。
         *
         * <p>返回值含义：
         * <ul>
         *   <li>SUCCESS —— 消费成功，继续消费下一条</li>
         *   <li>SUSPEND_CURRENT_QUEUE_A_MOMENT —— 暂停消费当前队列一会儿（稍后重试）</li>
         *   <li>注意：顺序消费<strong>没有 RECONSUME_LATER</strong>，因为一旦失败就不能跳过，
         *       否则会破坏顺序保证。所以顺序消费失败时只能暂停当前队列，等一会儿再试。</li>
         * </ul>
         *
         * @param messages 本次投递的顺序消息列表（来自同一个队列，按发送顺序排列）
         * @param context 顺序消费上下文（可设置自动提交等）
         * @return 固定返回 SUCCESS（示例代码不处理失败场景）
         */
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messages, ConsumeOrderlyContext context) {
            // 按顺序打印每条消息 —— 实际项目中这里是业务处理逻辑
            // 由于 RocketMQ 保证了同一队列内的消息按顺序投递到这里，
            // 所以 messages.get(0) 是队列中最早的消息，messages.get(n) 是最新的
            messages.forEach(MessageSupport::printMessage);
            // 返回 SUCCESS，表示本批次消息已成功消费
            return ConsumeOrderlyStatus.SUCCESS;
        }
    }
}
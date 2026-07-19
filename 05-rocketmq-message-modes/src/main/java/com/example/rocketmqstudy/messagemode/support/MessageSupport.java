package com.example.rocketmqstudy.messagemode.support;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

/**
 * 消息构建与打印工具 —— 统一示例的字符编码和观察字段。
 *
 * <p>为什么把"构建消息"和"打印消息"两个方法抽成工具类的原因：
 * <ul>
 *   <li>统一字符编码（UTF-8），避免各示例各自乱码问题</li>
 *   <li>统一打印格式（topic/tag/key/msgId/queueId/offset），方便观察和对比</li>
 *   <li>减少重复代码，所有示例都用同一套方法</li>
 * </ul>
 */
public final class MessageSupport {

    /**
     * 工具类不允许被实例化。
     */
    private MessageSupport() {
    }

    /**
     * 创建 RocketMQ 消息对象。
     *
     * <p>RocketMQ 消息的四个关键字段：
     * <pre>
     *   ┌─────────┬─────────────────────────────────────────┐
     *   │ 字段    │ 说明                                  │
     *   ├─────────┼─────────────────────────────────────┤
     *   │ Topic   │ 消息主题（必选）                        │
     *   │ Tag     │ 消息标签（可选，用于过滤）              │
     *   │ Key     │ 业务主键（可选，用于顺序消费和定位）    │
     *   │ Body    │ 消息正文（字节数组，必选）              │
     *   └─────────┴─────────────────────────────────────────┘
     * </pre>
     *
     * <p>字符编码：使用 StandardCharsets.UTF_8（Java 7+ 提供，比 "UTF-8" 字符串常量更安全）
     *
     * @param topic 消息主题（例如 "StudyNormalTopic"）。
     * @param tag 消息标签（例如 "BasicSync"，用于消费者过滤订阅）。
     * @param key 消息业务主键（用于顺序消费时，相同 key 的消息落到同一队列）。
     * @param body 消息正文（业务数据）。
     * @return 构建完成的 Message 对象，可直接传给 producer.send()。
     */
    public static Message buildMessage(String topic, String tag, String key, String body) {
        // new Message(topic, tag, key, byte[] body)
        // 把字符串 body 转成 UTF-8 字节数组 —— RocketMQ 的 body 只能存字节
        return new Message(topic, tag, key, body.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 打印消费消息的关键字段和正文。
     *
     * <p>打印的字段含义（从左到右）：
     * <pre>
     *   线程=...  ← 当前处理这条消息的线程名
     *   topic=...  ← 消息主题
     *   tag=...    ← 消息标签
     *   key=...    ← 业务主键
     *   msgId=...  ← RocketMQ 全局唯一 ID
     *   queueId=...  ← 所在队列编号（0~N-1）
     *   queueOffset=... ← 队列内的偏移量（从 0 开始递增）
     *   bornTime=...   ← 消息发送时间（时间戳，毫秒）
     *   body=...    ← 消息正文（UTF-8 解码后的字符串）
     * </pre>
     *
     * <p>注意：printMessage 方法接收的是 MessageExt（扩展消息），
     * 而 buildMessage 构建的是 Message（普通消息）。
     * 两者的关系：消费者从 Broker 拉取的消息都会被包装成 MessageExt，
     * MessageExt = Message + 额外元信息（msgId、queueId、bornTime 等）。
     *
     * @param message RocketMQ 扩展消息（从 consumer.poll() 或监听器拿到的消息）。
     */
    public static void printMessage(MessageExt message) {
        // 把字节数组 body 解码成 UTF-8 字符串（与 buildMessage 的编码对称）
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        // 用 printf 格式化打印，输出格式便于人眼阅读
        System.out.printf(
                "线程=%s, topic=%s, tag=%s, key=%s, msgId=%s, queueId=%d, queueOffset=%d, bornTime=%d, body=%s%n",
                Thread.currentThread().getName(),   // 当前线程名 —— 并发消费时能看到不同线程
                message.getTopic(),              // 消息主题
                message.getTags(),                 // 消息标签
                message.getKeys(),                // 业务主键
                message.getMsgId(),               // 消息唯一 ID
                message.getQueueId(),             // 队列编号（顺序消费时观察同一个 queueId）
                message.getQueueOffset(),         // 队列内的偏移量（观察消费进度）
                message.getBornTimestamp(),     // 消息发送时间（时间戳）
                body                              // 消息正文
        );
    }
}
package com.example.rocketmqstudy.messagemode.producer;

import com.example.rocketmqstudy.messagemode.config.MessageModeConfig;
import com.example.rocketmqstudy.messagemode.config.MessageScenario;
import com.example.rocketmqstudy.messagemode.support.MessageSupport;
import com.example.rocketmqstudy.messagemode.support.RocketMqClientFactory;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 事务消息生产者，演示半消息、本地事务状态提交以及 Broker 事务回查。
 */
public final class TransactionMessageProducer {

    /**
     * UNKNOWN_THEN_COMMIT 模式下默认等待 Broker 回查的秒数。
     */
    private static final int DEFAULT_CHECK_WAIT_SECONDS = 70;

    /**
     * 工具型入口类不允许被实例化。
     */
    private TransactionMessageProducer() {
    }

    /**
     * 程序入口，根据参数演示提交、回滚或先未知后回查提交。
     *
     * @param args 第一个参数为 commit、rollback 或 unknown，第二个参数可指定等待回查秒数。
     * @throws Exception 当客户端启动、事务消息发送或等待被中断时抛出。
     */
    public static void main(String[] args) throws Exception {
        TransactionDecision decision = TransactionDecision.fromArguments(args);
        int checkWaitSeconds = parseCheckWaitSeconds(args);
        MessageModeConfig config = MessageModeConfig.load();
        String topic = config.topicFor(MessageScenario.TRANSACTION);
        TransactionMQProducer producer = RocketMqClientFactory.createTransactionProducer(
                config, config.producerGroup("transaction"));
        producer.setTransactionListener(new DemoTransactionListener(decision));

        try {
            producer.start();
            Message message = MessageSupport.buildMessage(
                    topic, "CreateOrder", "transaction-order-3001", "创建订单 3001 并发送事务消息");
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, decision);
            System.out.println("事务消息发送结果：" + sendResult);

            if (decision == TransactionDecision.UNKNOWN_THEN_COMMIT) {
                System.out.println("本地事务首次返回 UNKNOW，等待 Broker 回查，最长等待秒数=" + checkWaitSeconds);
                new CountDownLatch(1).await(checkWaitSeconds, TimeUnit.SECONDS);
            }
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 解析回查等待时长。
     *
     * @param args 命令行参数。
     * @return 大于零的等待秒数。
     */
    private static int parseCheckWaitSeconds(String[] args) {
        int waitSeconds = args.length < 2 ? DEFAULT_CHECK_WAIT_SECONDS : Integer.parseInt(args[1]);
        if (waitSeconds <= 0) {
            throw new IllegalArgumentException("等待回查秒数必须大于 0");
        }
        return waitSeconds;
    }

    /**
     * 示例事务监听器，用内存状态模拟可被事务回查读取的本地事务结果。
     */
    private static final class DemoTransactionListener implements TransactionListener {

        /**
         * 本次示例采用的本地事务决策。
         */
        private final TransactionDecision decision;

        /**
         * 以业务 Key 保存最终本地事务状态，模拟数据库中的事务记录表。
         */
        private final Map<String, LocalTransactionState> localTransactionStates = new ConcurrentHashMap<>();

        /**
         * 创建示例事务监听器。
         *
         * @param decision 本次示例采用的本地事务决策。
         */
        private DemoTransactionListener(TransactionDecision decision) {
            this.decision = decision;
        }

        /**
         * 执行本地事务并返回首次事务状态。
         *
         * @param message Broker 已保存的半消息。
         * @param argument 发送事务消息时传入的事务决策。
         * @return 提交、回滚或未知状态。
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
            String businessKey = message.getKeys();
            System.out.println("执行本地事务，businessKey=" + businessKey + "，decision=" + decision.argumentName);

            if (decision == TransactionDecision.COMMIT) {
                localTransactionStates.put(businessKey, LocalTransactionState.COMMIT_MESSAGE);
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            if (decision == TransactionDecision.ROLLBACK) {
                localTransactionStates.put(businessKey, LocalTransactionState.ROLLBACK_MESSAGE);
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }

            localTransactionStates.put(businessKey, LocalTransactionState.COMMIT_MESSAGE);
            return LocalTransactionState.UNKNOW;
        }

        /**
         * 响应 Broker 回查，读取已记录的最终本地事务状态。
         *
         * @param messageExt 等待确认的事务消息。
         * @return 已知最终状态；未找到记录时继续返回未知。
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            String businessKey = messageExt.getKeys();
            LocalTransactionState state = localTransactionStates.getOrDefault(
                    businessKey, LocalTransactionState.UNKNOW);
            System.out.println("收到事务回查，businessKey=" + businessKey + "，返回状态=" + state);
            return state;
        }
    }

    /**
     * 示例支持的本地事务决策。
     */
    private enum TransactionDecision {

        /**
         * 本地事务成功并立即提交消息。
         */
        COMMIT("commit"),

        /**
         * 本地事务失败并回滚消息。
         */
        ROLLBACK("rollback"),

        /**
         * 首次返回未知，随后由事务回查确认提交。
         */
        UNKNOWN_THEN_COMMIT("unknown");

        /**
         * 命令行参数名称。
         */
        private final String argumentName;

        /**
         * 创建事务决策定义。
         *
         * @param argumentName 命令行参数名称。
         */
        TransactionDecision(String argumentName) {
            this.argumentName = argumentName;
        }

        /**
         * 从命令行参数解析事务决策。
         *
         * @param args 命令行参数。
         * @return 解析出的事务决策，默认提交。
         */
        private static TransactionDecision fromArguments(String[] args) {
            String value = args.length == 0 ? COMMIT.argumentName : args[0].toLowerCase(Locale.ROOT);
            for (TransactionDecision decision : values()) {
                if (decision.argumentName.equals(value)) {
                    return decision;
                }
            }
            throw new IllegalArgumentException("不支持的事务决策：" + value + "，可选值：commit、rollback、unknown");
        }
    }
}

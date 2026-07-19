package com.example.rocketmqstudy.springboot.transaction;

import com.example.rocketmqstudy.springboot.model.TransactionScenario;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 事务监听器的本地状态与 Header 读取测试。
 */
class TransactionMessageListenerTest {

    /**
     * 验证首次返回 UNKNOWN 后可通过 Broker 风格的前缀 Header 回查到 COMMIT。
     */
    @Test
    void shouldCommitWhenUnknownTransactionIsCheckedLater() {
        LocalTransactionService transactionService = new LocalTransactionService();
        TransactionMessageListener listener = new TransactionMessageListener(transactionService);
        Message<String> message = MessageBuilder.withPayload("transaction-body")
                .setHeader(RocketMQHeaders.PREFIX + RocketMQHeaders.KEYS, "TX-TEST-001")
                .build();

        RocketMQLocalTransactionState firstState = listener.executeLocalTransaction(
                message, TransactionScenario.UNKNOWN_THEN_COMMIT);
        RocketMQLocalTransactionState checkedState = listener.checkLocalTransaction(message);

        assertEquals(RocketMQLocalTransactionState.UNKNOWN, firstState);
        assertEquals(RocketMQLocalTransactionState.COMMIT, checkedState);
    }
}

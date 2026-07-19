package com.example.rocketmqstudy.springboot.transaction;

import com.example.rocketmqstudy.springboot.model.TransactionScenario;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;

/**
 * 事务消息的本地事务执行与回查监听器。
 */
@RocketMQTransactionListener(corePoolSize = 2, maximumPoolSize = 4)
public class TransactionMessageListener implements RocketMQLocalTransactionListener {

    /** 本地事务业务服务。 */
    private final LocalTransactionService localTransactionService;

    /**
     * 创建事务消息监听器。
     *
     * @param localTransactionService 本地事务业务服务。
     */
    public TransactionMessageListener(LocalTransactionService localTransactionService) {
        this.localTransactionService = localTransactionService;
    }

    /**
     * 执行本地事务，并决定提交、回滚或等待回查。
     *
     * @param message RocketMQ 半消息。
     * @param argument 发送时传入的事务场景。
     * @return 本地事务执行状态。
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object argument) {
        String transactionKey = readTransactionKey(message);
        TransactionScenario scenario = (TransactionScenario) argument;
        RocketMQLocalTransactionState state = localTransactionService.execute(transactionKey, scenario);
        System.out.printf("执行本地事务：key=%s, scenario=%s, returnedState=%s%n",
                transactionKey, scenario, state);
        return state;
    }

    /**
     * 响应 Broker 回查并返回本地事务最终状态。
     *
     * @param message 被回查的事务消息。
     * @return 本地事务最终状态。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        String transactionKey = readTransactionKey(message);
        RocketMQLocalTransactionState state = localTransactionService.check(transactionKey);
        System.out.printf("回查本地事务：key=%s, state=%s%n", transactionKey, state);
        return state;
    }

    /**
     * 从 Spring Message 消息头读取业务事务 Key。
     *
     * @param message RocketMQ 事务消息。
     * @return 业务事务 Key。
     */
    private String readTransactionKey(Message message) {
        Object headerValue = message.getHeaders().get(RocketMQHeaders.PREFIX + RocketMQHeaders.KEYS);
        if (headerValue == null) {
            headerValue = message.getHeaders().get(RocketMQHeaders.KEYS);
        }
        if (headerValue == null) {
            throw new IllegalArgumentException("事务消息缺少 RocketMQ KEYS 消息头");
        }
        return headerValue.toString();
    }
}

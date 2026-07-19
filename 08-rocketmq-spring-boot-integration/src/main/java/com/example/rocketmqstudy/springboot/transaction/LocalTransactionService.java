package com.example.rocketmqstudy.springboot.transaction;

import com.example.rocketmqstudy.springboot.model.TransactionScenario;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 演示本地事务执行和状态查询的服务。
 *
 * <p>内存状态仅用于观察事务回查流程，进程重启会丢失，不是生产实现。</p>
 */
@Service
public class LocalTransactionService {

    /** 事务 Key 与最终本地事务状态的内存映射。 */
    private final ConcurrentMap<String, RocketMQLocalTransactionState> transactionStates =
            new ConcurrentHashMap<>();

    /**
     * 根据演示场景执行本地事务。
     *
     * @param transactionKey 事务业务 Key。
     * @param scenario 演示场景。
     * @return 首次返回给 Broker 的本地事务状态。
     */
    public RocketMQLocalTransactionState execute(String transactionKey, TransactionScenario scenario) {
        if (scenario == TransactionScenario.COMMIT) {
            transactionStates.put(transactionKey, RocketMQLocalTransactionState.COMMIT);
            return RocketMQLocalTransactionState.COMMIT;
        }
        if (scenario == TransactionScenario.ROLLBACK) {
            transactionStates.put(transactionKey, RocketMQLocalTransactionState.ROLLBACK);
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        // 模拟首次无法确定，但业务数据库中的最终状态已经可供后续回查。
        transactionStates.put(transactionKey, RocketMQLocalTransactionState.COMMIT);
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * 查询本地事务最终状态。
     *
     * @param transactionKey 事务业务 Key。
     * @return 已记录状态；找不到记录时返回 UNKNOWN，避免错误提交消息。
     */
    public RocketMQLocalTransactionState check(String transactionKey) {
        return transactionStates.getOrDefault(transactionKey, RocketMQLocalTransactionState.UNKNOWN);
    }
}

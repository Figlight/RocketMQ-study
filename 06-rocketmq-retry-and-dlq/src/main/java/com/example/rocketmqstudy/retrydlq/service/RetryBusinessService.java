package com.example.rocketmqstudy.retrydlq.service;

import com.example.rocketmqstudy.retrydlq.exception.RecoverableBusinessException;
import com.example.rocketmqstudy.retrydlq.exception.UnrecoverableBusinessException;
import com.example.rocketmqstudy.retrydlq.model.FailureType;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 重试示例业务服务，集中封装演示用的成功与失败规则。
 */
public final class RetryBusinessService {

    /** 第三次投递对应的重新消费次数。 */
    private static final int THIRD_DELIVERY_RECONSUME_TIMES = 2;

    /**
     * 前两次投递抛出可恢复异常，第三次投递处理成功。
     *
     * @param message 当前消息。
     * @throws RecoverableBusinessException 当前仍处于前两次投递时抛出。
     */
    public void processUntilThirdDelivery(MessageExt message) throws RecoverableBusinessException {
        if (message.getReconsumeTimes() < THIRD_DELIVERY_RECONSUME_TIMES) {
            throw new RecoverableBusinessException("模拟下游服务暂时不可用");
        }
        System.out.println("业务处理成功：第三次投递满足恢复条件");
    }

    /**
     * 按失败类型持续制造业务失败。
     *
     * @param failureType 失败类型。
     * @throws RecoverableBusinessException 可恢复失败。
     * @throws UnrecoverableBusinessException 不可恢复失败。
     */
    public void processAlwaysFail(FailureType failureType)
            throws RecoverableBusinessException, UnrecoverableBusinessException {
        if (failureType == FailureType.UNRECOVERABLE) {
            throw new UnrecoverableBusinessException("模拟消息格式非法，重复执行也无法修复");
        }
        throw new RecoverableBusinessException("模拟依赖服务持续不可用");
    }
}

package com.example.rocketmqstudy.retrydlq.producer;

import com.example.rocketmqstudy.retrydlq.config.RetryAndDlqConfig;
import com.example.rocketmqstudy.retrydlq.model.FailureType;
import com.example.rocketmqstudy.retrydlq.support.MessageSupport;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 发送重试生产者，演示同步与异步发送失败重试配置。
 */
public final class RetryMessageProducer {

    /** 异步回调最长等待秒数。 */
    private static final int CALLBACK_TIMEOUT_SECONDS = 10;

    /** 工具型入口类不允许实例化。 */
    private RetryMessageProducer() {
    }

    /**
     * 启动生产者并按参数发送消息。
     *
     * @param args 第一个参数为 sync 或 async；第二个参数为 success、recoverable 或 unrecoverable。
     * @throws Exception 客户端启动、发送或回调失败时抛出。
     */
    public static void main(String[] args) throws Exception {
        SendMode sendMode = SendMode.fromArguments(args);
        String scenario = parseScenario(args);
        RetryAndDlqConfig config = RetryAndDlqConfig.load();
        DefaultMQProducer producer = new DefaultMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getNamesrvAddr());

        // 同步 send(...) 发生客户端可重试故障时，最多额外尝试指定次数。
        producer.setRetryTimesWhenSendFailed(config.getSyncRetryTimes());
        // 异步 send(..., callback) 发生客户端可重试故障时，最多额外尝试指定次数。
        producer.setRetryTimesWhenSendAsyncFailed(config.getAsyncRetryTimes());

        try {
            // 启动生产者
            producer.start();
            // 构建消息
            Message message = buildScenarioMessage(config, scenario);
            System.out.printf("发送配置：mode=%s, syncRetryTimes=%d, asyncRetryTimes=%d, timeoutMillis=%d%n",
                    sendMode.argumentName, config.getSyncRetryTimes(), config.getAsyncRetryTimes(),
                    config.getSendTimeoutMillis());
            if (sendMode == SendMode.SYNC) {
                sendSynchronously(producer, message, config.getSendTimeoutMillis());
            } else {
                sendAsynchronously(producer, message, config.getSendTimeoutMillis());
            }
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 构建指定消费场景的消息。
     *
     * @param config 运行配置。
     * @param scenario 消费场景。
     * @return 待发送消息。
     */
    private static Message buildScenarioMessage(RetryAndDlqConfig config, String scenario) {
        String tag = switch (scenario) {
            case "success" -> "RetrySuccess";
            case "recoverable" -> "AlwaysFailRecoverable";
            case "unrecoverable" -> "AlwaysFailUnrecoverable";
            default -> throw new IllegalStateException("未处理的场景：" + scenario);
        };
        return MessageSupport.buildMessage(config.getTopic(), tag,
                scenario + "-" + System.currentTimeMillis(), "重试与死信演示：" + scenario);
    }

    /**
     * 同步发送消息。
     *
     * @param producer 已启动生产者。
     * @param message 待发送消息。
     * @param timeoutMillis 单次发送超时时间。
     * @throws Exception 发送失败时抛出。
     */
    private static void sendSynchronously(DefaultMQProducer producer, Message message, long timeoutMillis)
            throws Exception {
        SendResult result = producer.send(message, timeoutMillis);
        System.out.println("同步发送成功：" + result);
    }

    /**
     * 异步发送消息并等待演示回调完成。
     *
     * @param producer 已启动生产者。
     * @param message 待发送消息。
     * @param timeoutMillis 单次发送超时时间。
     * @throws Exception 发送调用、回调或等待失败时抛出。
     */
    private static void sendAsynchronously(DefaultMQProducer producer, Message message, long timeoutMillis)
            throws Exception {
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        producer.send(message, new SendCallback() {
            /** @param sendResult Broker 返回的发送结果。 */
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步发送成功：" + sendResult);
                callbackLatch.countDown();
            }

            /** @param throwable 异步发送最终失败原因。 */
            @Override
            public void onException(Throwable throwable) {
                callbackFailure.set(throwable);
                callbackLatch.countDown();
            }
        }, timeoutMillis);
        System.out.println("异步发送调用已返回，客户端会按配置执行发送失败重试");
        if (!callbackLatch.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("等待异步发送回调超时");
        }
        if (callbackFailure.get() != null) {
            throw new IllegalStateException("异步发送最终失败", callbackFailure.get());
        }
    }

    /**
     * 解析消费演示场景。
     *
     * @param args 命令行参数。
     * @return success、recoverable 或 unrecoverable。
     */
    private static String parseScenario(String[] args) {
        String scenario = args.length < 2 ? "success" : args[1].toLowerCase(Locale.ROOT);
        if ("success".equals(scenario)) {
            return scenario;
        }
        return FailureType.fromArguments(new String[]{scenario}).getArgumentName();
    }

    /**
     * 生产者发送模式。
     */
    private enum SendMode {

        /** 同步发送。 */
        SYNC("sync"),

        /** 异步发送。 */
        ASYNC("async");

        /** 命令行参数值。 */
        private final String argumentName;

        /**
         * 创建发送模式。
         *
         * @param argumentName 命令行参数值。
         */
        SendMode(String argumentName) {
            this.argumentName = argumentName;
        }

        /**
         * 解析发送模式。
         *
         * @param args 命令行参数。
         * @return 发送模式。
         */
        private static SendMode fromArguments(String[] args) {
            String value = args.length == 0 ? SYNC.argumentName : args[0].toLowerCase(Locale.ROOT);
            for (SendMode mode : values()) {
                if (mode.argumentName.equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("发送模式只支持 sync 或 async：" + value);
        }
    }
}

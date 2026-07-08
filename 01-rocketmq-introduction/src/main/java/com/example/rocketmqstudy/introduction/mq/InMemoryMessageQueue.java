package com.example.rocketmqstudy.introduction.mq;

import com.example.rocketmqstudy.introduction.consumer.OrderEventConsumer;
import com.example.rocketmqstudy.introduction.model.OrderPlacedEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.example.rocketmqstudy.introduction.support.DemoLogger.log;

/**
 * 内存版 MQ，负责暂存消息并把消息异步转发给消费者。
 */
public final class InMemoryMessageQueue implements AutoCloseable {

    /**
     * 消息队列，用于缓冲生产者发送过来的订单事件。
     */
    private final BlockingQueue<OrderPlacedEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * 消费者列表，用于模拟发布订阅模式下一个消息被多个系统处理。
     */
    private final List<OrderEventConsumer> consumers = new ArrayList<>();

    /**
     * 消费者线程池，用于在后台异步处理队列中的消息。
     */
    private final ExecutorService workerPool = Executors.newSingleThreadExecutor();

    /**
     * 运行标记，用于控制后台消费者循环是否继续执行。
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 注册一个消费者。
     *
     * @param consumer 需要订阅订单事件的消费者。
     */
    public void registerConsumer(OrderEventConsumer consumer) {
        consumers.add(consumer);
    }

    /**
     * 启动后台消费线程。
     */
    public void start() {
        running.set(true);
        workerPool.submit(this::consumeLoop);
    }

    /**
     * 发布订单事件到队列中，模拟生产者把消息发送给 MQ。
     *
     * @param event 订单创建事件。
     */
    public void publish(OrderPlacedEvent event) {
        eventQueue.offer(event);
        log("MQ 收到消息：" + event.getOrderId() + "，当前积压数量：" + eventQueue.size());
    }

    /**
     * 后台消费循环，从队列中取出消息并交给所有消费者处理。
     */
    private void consumeLoop() {
        while (running.get() || !eventQueue.isEmpty()) {
            try {
                OrderPlacedEvent event = eventQueue.poll(200, TimeUnit.MILLISECONDS);
                if (event != null) {
                    dispatch(event);
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                running.set(false);
            }
        }
    }

    /**
     * 把一条消息分发给所有已注册消费者。
     *
     * @param event 需要被消费的订单事件。
     * @throws InterruptedException 当消费者模拟处理耗时时被中断。
     */
    private void dispatch(OrderPlacedEvent event) throws InterruptedException {
        for (OrderEventConsumer consumer : consumers) {
            consumer.consume(event);
        }
    }

    /**
     * 关闭内存 MQ，并等待已经进入队列的消息处理完成。
     *
     * @throws InterruptedException 当等待线程池关闭时被中断。
     */
    @Override
    public void close() throws InterruptedException {
        running.set(false);
        workerPool.shutdown();
        if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
            workerPool.shutdownNow();
        }
    }
}

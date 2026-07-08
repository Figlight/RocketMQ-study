# 01 RocketMQ 简介

## 模块文件结构

```text
introduction/
├── IntroductionDemo.java              # 程序入口（main 方法）
├── model/
│   └── OrderPlacedEvent.java          # 订单事件消息体
├── mq/
│   └── InMemoryMessageQueue.java      # 内存版 MQ（核心）
├── service/
│   └── OrderSystem.java               # 生产者（订单系统）
├── consumer/
│   ├── OrderEventConsumer.java        # 消费者接口
│   ├── InventoryConsumer.java         # 消费者1：扣减库存
│   ├── CouponConsumer.java            # 消费者2：发送优惠券
│   └── PointConsumer.java             # 消费者3：增加积分
└── support/
    └── DemoLogger.java                # 带时间戳的日志工具
```

## 本章对应文档内容

文档第一章主要说明：

- MQ 是 Message Queue，即消息队列。
- RocketMQ 是一个分布式消息系统，提供低延时、高可靠的消息发布与订阅能力。
- MQ 的核心价值是异步、解耦、削峰限流。
- 发送者把消息交给 MQ 后，可以不直接依赖接收者的处理过程。

## 示例说明

本章示例没有直接连接真实 RocketMQ，而是用 JDK 内置的 `BlockingQueue` 做一个内存版 MQ，帮助理解消息队列的三个作用：

- 异步：下单系统把事件放入队列后立即返回。
- 解耦：下单系统不直接调用库存、积分、优惠券系统。
- 削峰：突发订单先进入队列，后台消费者按自己的速度慢慢处理。

## 运行方式

在项目根目录执行：

```bash
mvn -q -pl 01-rocketmq-introduction package
java -cp 01-rocketmq-introduction/target/classes com.example.rocketmqstudy.introduction.IntroductionDemo
```

## 注意点

- 这个示例只是概念演示，不代表 RocketMQ 的真实实现。
- 真实 RocketMQ 的队列持久化、Broker、NameServer、消费进度等能力会更复杂。
- 学习时重点观察：生产者发送消息后不等待业务处理完成，消费者在后台异步处理消息。

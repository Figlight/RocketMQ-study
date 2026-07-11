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

## 流程图

```text
用户下单请求
    |
    v
+-------------------------+
| OrderSystem             |
| 订单系统                 |
| 只负责创建订单事件        |
+-------------------------+
    |
    | publish(event)
    v
+-------------------------+
| InMemoryMessageQueue    |
| 内存版 MQ                |
| BlockingQueue 暂存消息   |
+-------------------------+
    |
    | 后台线程异步取消息
    v
+---------------------------------------------------+
| dispatch(event)                                   |
| 一条订单事件被分发给多个订阅者                     |
+---------------------------------------------------+
      |                    |                    |
      v                    v                    v
+-------------+     +-------------+     +-------------+
| 库存消费者   |     | 优惠券消费者 |     | 积分消费者   |
| 扣减库存     |     | 发送优惠券   |     | 增加积分     |
+-------------+     +-------------+     +-------------+
      |                    |                    |
      v                    v                    v
  异步完成库存         异步完成营销         异步完成积分
```

## 本章对应文档内容

文档第一章主要说明：

- MQ 是 Message Queue，即消息队列。
- RocketMQ 是一个分布式消息系统，提供低延时、高可靠的消息发布与订阅能力。
- MQ 的核心价值是异步、解耦、削峰限流。
- 发送者把消息交给 MQ 后，可以不直接依赖接收者的处理过程。

## 核心理解

MQ 可以先理解成“消息中转站”。生产者把消息交给 MQ，消费者再从 MQ 中取消息处理。生产者不需要直接知道消费者是谁，也不需要等待消费者把业务处理完。

文档里强调 MQ 的三个主要作用：

1. 异步：把非核心链路拆出去。比如下单成功后，库存、优惠券、积分可以慢慢处理，不阻塞下单主流程。
2. 解耦：订单系统不直接依赖库存系统、积分系统、营销系统。以后新增一个短信通知系统，只需要新增一个消费者，不必大改订单系统。
3. 削峰限流：突发请求先进入队列，消费者按自己的处理能力慢慢消费，避免后端系统被瞬间冲垮。

本章先不连接真实 RocketMQ，是因为第一章重点不是 API，而是先建立“为什么需要消息队列”的直觉。

## 示例说明

本章示例没有直接连接真实 RocketMQ，而是用 JDK 内置的 `BlockingQueue` 做一个内存版 MQ，帮助理解消息队列的三个作用：

- 异步：下单系统把事件放入队列后立即返回。
- 解耦：下单系统不直接调用库存、积分、优惠券系统。
- 削峰：突发订单先进入队列，后台消费者按自己的速度慢慢处理。

示例中的角色和真实 RocketMQ 的对应关系：

| 示例角色 | 对应概念 | 说明 |
| --- | --- | --- |
| `OrderSystem` | Producer | 订单系统产生订单事件，类似消息生产者 |
| `OrderPlacedEvent` | Message | 一条订单创建消息 |
| `InMemoryMessageQueue` | MQ / Broker 的简化版 | 暂存消息，并异步交给消费者 |
| `InventoryConsumer` | Consumer | 消费订单事件并扣减库存 |
| `CouponConsumer` | Consumer | 消费订单事件并发送优惠券 |
| `PointConsumer` | Consumer | 消费订单事件并增加积分 |

运行时重点观察日志顺序：`OrderSystem` 很快把 5 个订单都交给 MQ，而库存、优惠券、积分是在后面逐步完成的。这就是异步带来的效果。

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
- 本章的 `BlockingQueue` 只存在于内存中，程序结束后消息会丢失；真实 RocketMQ 会把消息写入 Broker 存储。
- 当前示例为了演示发布订阅效果，让一条消息被多个消费者都处理；后续章节会继续区分集群消费、广播消费等模式。

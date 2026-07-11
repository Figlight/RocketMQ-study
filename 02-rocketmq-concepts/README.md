# 02 RocketMQ 概念

## 模块文件结构

```text
concepts/
├── RocketMqConceptDemo.java           # 程序入口（main 方法）
├── model/
│   └── Message.java                   # 消息对象，包含 Topic、Key、Body
├── nameserver/
│   └── NameServer.java                # NameServer：维护 Topic 到 Broker 的路由
├── broker/
│   ├── Broker.java                    # Broker：创建 Topic、写入消息、维护消费进度
│   └── MessageQueue.java              # Queue：保存消息并按 offset 拉取
├── producer/
│   └── Producer.java                  # Producer：查询路由并发送消息
└── consumer/
    └── Consumer.java                  # Consumer：按消费者组和 Queue 拉取消息
```

## 流程图

```text
                           +----------------------------------+
                           | NameServer 路由中心              |
                           | topicRouteTable                  |
                           | "order-topic" -> broker-a        |
                           +----------------------------------+
                                      ^
                                      |
                         查询路由     |     注册路由
                                      |
+----------------+          +-------------------------------+
| Producer       |---------> | Broker                        |
| producerGroup  | 写入消息  | topicQueueTable               |
| send()         |          | "order-topic" -> [Q0, Q1]     |
+----------------+          | consumerOffsetTable            |
                            | 记录各 ConsumerGroup 消费进度  |
                            +-------------------------------+
                                      |
                    按 Queue 和 ConsumerGroup 拉取消息
                                      |
          +---------------------------+---------------------------+
          |                           |                           |
          v                           v                           v
+------------------+        +------------------+        +------------------+
| Consumer A       |        | Consumer B       |        | Consumer C       |
| group=order      |        | group=order      |        | group=audit      |
| queueId=0        |        | queueId=1        |        | queueId=0        |
+------------------+        +------------------+        +------------------+
          |                           |                           |
          v                           v                           v
消费 KEY-1、KEY-3          消费 KEY-2、KEY-4          独立消费 KEY-1、KEY-3
订单业务消费者组            订单业务消费者组            审计业务消费者组
```

## 本章对应文档内容

文档第二章主要说明 RocketMQ 的核心角色：

- Producer：消息发送者，也叫生产者。
- Consumer：消息接收者，也叫消费者。
- Broker：暂存和传输消息的服务节点。
- NameServer：Broker 的注册中心，保存 Broker 路由信息。
- Queue：消息实际存放的位置，一个 Topic 可以有多个 Queue。
- Topic：消息分类。
- ProducerGroup：生产者组。
- ConsumerGroup：消费者组，不同消费者组可以各自消费同一个 Topic。

## 核心概念说明

RocketMQ 不是只有“一个队列”这么简单，它由多个角色配合完成消息投递。

| 概念 | 本章理解 | 类比 |
| --- | --- | --- |
| Producer | 发送消息的一方 | 发件人 |
| Consumer | 接收并处理消息的一方 | 收件人 |
| Broker | 真正保存消息、转发消息的服务 | 快递站点 |
| NameServer | 保存 Broker 路由信息 | 快递网点查询中心 |
| Topic | 消息分类 | 订单消息、支付消息、物流消息 |
| Queue | Topic 下实际存放消息的位置 | 一个 Topic 下的多个分片队列 |
| ProducerGroup | 一组生产者 | 订单服务集群 |
| ConsumerGroup | 一组消费者 | 库存服务集群、审计服务集群 |

文档中的重点流程可以拆成三步：

1. Broker 启动后，把自己的信息注册到 NameServer。
2. Producer 发送消息前，先问 NameServer：这个 Topic 应该发到哪个 Broker？
3. Consumer 消费消息前，也问 NameServer：这个 Topic 应该从哪个 Broker 拉取？

所以 Producer 和 Consumer 不需要直接认识对方，它们都通过 NameServer 找到 Broker，再围绕 Broker 完成消息发送和消费。

## 示例说明

本章示例用内存对象模拟 RocketMQ 的基本流转：

1. Broker 创建 Topic 和 Queue。
2. Broker 把 Topic 路由注册到 NameServer。
3. Producer 发送消息前先从 NameServer 查询 Broker。
4. Broker 将消息按轮询方式写入不同 Queue。
5. Consumer 根据 ConsumerGroup 拉取消息。
6. 不同 ConsumerGroup 拥有独立消费进度。

示例中有一个容易混淆但很重要的点：`order-consumer-group` 和 `audit-consumer-group` 是两个不同的消费者组。它们消费同一个 Topic 时，消费进度互不影响。

- `order-consumer-a` 负责 Queue0，消费 `KEY-1`、`KEY-3`。
- `order-consumer-b` 负责 Queue1，消费 `KEY-2`、`KEY-4`。
- `audit-consumer-a` 属于另一个组，也能从 Queue0 重新消费 `KEY-1`、`KEY-3`。

这可以帮助理解：同一个 Topic 可以被多个业务系统订阅；同一个 ConsumerGroup 内部通常是分摊消费，不同 ConsumerGroup 之间是独立消费。

## 运行方式

在项目根目录执行：

```bash
mvn -q -pl 02-rocketmq-concepts package
java -cp 02-rocketmq-concepts/target/classes com.example.rocketmqstudy.concepts.RocketMqConceptDemo
```

## 注意点

- 这个示例用于理解概念，不是 RocketMQ 客户端源码。
- 真实 RocketMQ 的路由、队列分配、消费位点、重试、刷盘和高可用都更复杂。
- 学习时重点观察：Producer 和 Consumer 都不直接持有对方地址，而是通过 NameServer 找到 Broker。
- 本章的 `Broker` 用内存 Map 保存消息和消费进度；真实 RocketMQ 会把消息和消费位点持久化。
- `MessageQueue` 在本章是包内类，因为它只服务于 `Broker`，外部模块不需要直接操作它。

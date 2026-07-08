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

## 示例说明

本章示例用内存对象模拟 RocketMQ 的基本流转：

1. Broker 创建 Topic 和 Queue。
2. Broker 把 Topic 路由注册到 NameServer。
3. Producer 发送消息前先从 NameServer 查询 Broker。
4. Broker 将消息按轮询方式写入不同 Queue。
5. Consumer 根据 ConsumerGroup 拉取消息。
6. 不同 ConsumerGroup 拥有独立消费进度。

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

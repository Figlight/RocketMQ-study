# 04 RocketMQ 快速入门

## 模块文件结构

```text
quickstart/
├── config/
│   ├── QuickStartConfig.java          # 读取 NameServer、Topic、Group 等运行配置
│   └── QuickStartConstants.java       # 快速入门示例公共常量
├── producer/
│   └── QuickStartProducer.java        # 同步消息生产者
└── consumer/
    └── QuickStartConsumer.java        # PushConsumer 消息消费者
```

## 流程图

```text
启动消费者
  |
  |-- 创建 DefaultMQPushConsumer
  |-- 设置 NameServer 地址
  |-- 订阅 TopicTest + TagA
  |-- 注册 MessageListenerConcurrently
  v
等待 Broker 推送消息

启动生产者
  |
  |-- 创建 DefaultMQProducer
  |-- 设置 NameServer 地址
  |-- 启动 Producer
  |-- 构建 Message(topic, tag, key, body)
  |-- producer.send(message)
  v
Broker 返回 SendResult
  |
  v
消费者监听器收到消息并返回 CONSUME_SUCCESS
```

## 本章对应文档内容

文档第四章主要说明：

- RocketMQ 支持同步消息、异步消息、顺序消息、延迟消息、事务消息等多种发送模式。
- 快速入门先从普通同步消息开始。
- 生产者流程：创建生产者、指定 NameServer、启动生产者、创建消息、发送消息、关闭生产者。
- 消费者流程：创建消费者、指定 NameServer、订阅 Topic 和 Tag、注册监听器、处理消息、启动消费者。
- 消费失败时可以返回 `RECONSUME_LATER`，消息会稍后重新投递。

## 核心理解

快速入门阶段重点不是复杂特性，而是先跑通真实 RocketMQ 客户端的最短链路：

1. NameServer 已经启动，并监听 `9876` 端口。
2. Broker 已经启动，并注册到 NameServer。
3. Producer 通过 NameServer 找到 Broker，把消息发送到 `TopicTest`。
4. Consumer 通过 NameServer 找到 Broker，订阅 `TopicTest` 并监听消息。
5. 消费者处理成功后返回 `CONSUME_SUCCESS`，Broker 记录消费进度。

本章和前两章的区别是：前两章使用内存对象模拟概念，本章开始连接真实 RocketMQ 服务。

## 示例说明

### 生产者

`QuickStartProducer` 对应文档中的“编写生产者”：

1. 创建 `DefaultMQProducer`，并指定生产者组。
2. 设置 `namesrvAddr`。
3. 调用 `start()` 启动实例。
4. 构建 `Message`，指定 Topic、Tag、Key 和 Body。
5. 调用 `send()` 同步发送消息。
6. 在 `finally` 中关闭生产者。

同步发送会等待 Broker 返回 `SendResult`。如果控制台看到 `SEND_OK`，说明 Broker 已经确认收到消息。

### 消费者

`QuickStartConsumer` 对应文档中的“编写消费者”：

1. 创建 `DefaultMQPushConsumer`，并指定消费者组。
2. 设置 `namesrvAddr`。
3. 订阅 `TopicTest` 和 `TagA`。
4. 注册 `MessageListenerConcurrently` 并发消费监听器。
5. 启动消费者并持续等待消息。

消费者默认返回 `CONSUME_SUCCESS`。如果想观察文档里提到的重试效果，可以开启 `ROCKETMQ_FORCE_RETRY=true`，此时监听器会返回 `RECONSUME_LATER`，Broker 会稍后重新投递消息。

## 前置条件

需要先按第三章启动 RocketMQ：

- NameServer 地址：`127.0.0.1:9876`
- Broker 已成功连接 NameServer
- Broker 开启了 `autoCreateTopicEnable=true`，或者已经提前创建 `TopicTest`

如果你使用 Docker 方式启动，本章默认配置可以直接连接第三章推荐的本地环境。

### 本机 Docker 配置提醒

如果本机同时要使用 Java Demo 和 RocketMQ Dashboard，需要特别注意 `brokerIP1`。这个地址会被 Broker 注册到 NameServer，Producer、Consumer、Dashboard 都会从 NameServer 拿到它，再用它连接 Broker。

本机排查时验证过：

- `brokerIP1=127.0.0.1`：宿主机 Java Demo 容易访问，但 Docker 容器内的 Dashboard 会把 `127.0.0.1` 当成 Dashboard 容器自己，可能访问不到 Broker。
- `brokerIP1=host.docker.internal`：Dashboard 容器可以访问，但宿主机 Java Demo 可能访问不稳定或超时。
- `brokerIP1=192.168.1.6`：当前机器上宿主机 Java Demo 和 Dashboard 容器都可以访问。

因此当前本机学习环境建议在 `03-rocketmq-installation/docker/broker.conf` 中保持：

```properties
brokerIP1=192.168.1.6
autoCreateTopicEnable=true
```

注意：`192.168.1.6` 是当前电脑的 WLAN IPv4 地址。如果以后换了网络，本机 IP 变化，需要同步修改 `brokerIP1`。

重建 Broker 时建议带上 `JAVA_OPT_EXT` 参数，避免当前 Docker/JDK 组合下 Broker 拉消息时出现 `StoreUtil` 初始化异常：

```powershell
docker rm -f rmqbroker

docker run -d `
  --name rmqbroker `
  --net rocketmq `
  -p 10912:10912 -p 10911:10911 -p 10909:10909 `
  -e "NAMESRV_ADDR=rmqnamesrv:9876" `
  -e "JAVA_OPT_EXT=-XX:-UseContainerSupport" `
  -v "D:\vibe-coding-project\RocketMQ-study\03-rocketmq-installation\docker\broker.conf:/home/rocketmq/rocketmq-4.9.6/conf/broker.conf" `
  apache/rocketmq:4.9.6 sh mqbroker `
  -c /home/rocketmq/rocketmq-4.9.6/conf/broker.conf
```

启动后先确认 Broker 是 `Up`：

```powershell
docker ps
docker logs --tail 20 rmqbroker
```

日志中需要看到：

```text
The broker[broker-a, 192.168.1.6:10911] boot success
```

如果重建 Broker 后 `TopicTest` 不存在，可以手动创建：

```powershell
docker exec rmqbroker sh -c "cd /home/rocketmq/rocketmq-4.9.6 && sh bin/mqadmin updateTopic -n rmqnamesrv:9876 -b 192.168.1.6:10911 -t TopicTest -r 4 -w 4"
```

## 运行方式

### 1. 编译本章

在项目根目录执行：

```bash
mvn -q -pl 04-rocketmq-quickstart package
```

### 2. 启动消费者

先启动消费者，让它订阅 Topic 并等待消息：

```bash
mvn -q -pl 04-rocketmq-quickstart exec:java -Dexec.mainClass=com.example.rocketmqstudy.quickstart.consumer.QuickStartConsumer
```

### 3. 启动生产者

再打开一个终端启动生产者：

```bash
mvn -q -pl 04-rocketmq-quickstart exec:java -Dexec.mainClass=com.example.rocketmqstudy.quickstart.producer.QuickStartProducer
```

默认发送 10 条消息。也可以传入消息条数：

```bash
mvn -q -pl 04-rocketmq-quickstart exec:java -Dexec.mainClass=com.example.rocketmqstudy.quickstart.producer.QuickStartProducer -Dexec.args="3"
```

## 可配置项

示例会优先读取 JVM 系统属性，其次读取环境变量：

| 配置项 | JVM 系统属性 | 环境变量 | 默认值 |
| --- | --- | --- | --- |
| NameServer 地址 | `rocketmq.namesrvAddr` | `ROCKETMQ_NAMESRV_ADDR` | `127.0.0.1:9876` |
| Topic | `rocketmq.topic` | `ROCKETMQ_TOPIC` | `TopicTest` |
| Tag | `rocketmq.tag` | `ROCKETMQ_TAG` | `TagA` |
| 生产者组 | `rocketmq.producerGroup` | `ROCKETMQ_PRODUCER_GROUP` | `quickstart-producer-group` |
| 消费者组 | `rocketmq.consumerGroup` | `ROCKETMQ_CONSUMER_GROUP` | `quickstart-consumer-group` |
| 是否演示重试 | `rocketmq.forceRetry` | `ROCKETMQ_FORCE_RETRY` | `false` |

示例：

```bash
mvn -q -pl 04-rocketmq-quickstart exec:java -Dexec.mainClass=com.example.rocketmqstudy.quickstart.producer.QuickStartProducer -Drocketmq.namesrvAddr=192.168.1.10:9876
```

## 排坑记录

### 1. 生产者报 `sendDefaultImpl call timeout`

现象：

```text
org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException: sendDefaultImpl call timeout
```

含义：Producer 已经启动，但发送消息时没有在超时时间内拿到 Broker 响应。

本机遇到的原因是 `brokerIP1` 不合适。Producer 不是直接把消息发给 `namesrvAddr`，而是先从 NameServer 查询 Topic 路由，再连接路由里的 Broker 地址。这个 Broker 地址来自 `broker.conf` 的 `brokerIP1`。

处理方式：

1. 检查 Broker 是否启动。
2. 检查 `brokerIP1` 是否是宿主机 Java 程序能访问的地址。
3. 当前本机使用 `brokerIP1=192.168.1.6`。

### 2. 生产者报 `No route info of this topic: TopicTest`

现象：

```text
org.apache.rocketmq.client.exception.MQClientException: No route info of this topic: TopicTest
```

含义：NameServer 里没有 `TopicTest` 的可用路由。

常见原因：

- Broker 没启动成功，只是 `Created`，不是 `Up`。
- Broker 启动时挂载的 `broker.conf` 路径写错。
- 重建 Broker 后旧 Topic 数据没了，`TopicTest` 尚未创建。
- Broker 没有开启 `autoCreateTopicEnable=true`。

排查命令：

```powershell
docker ps -a
docker logs --tail 50 rmqbroker
docker exec rmqbroker sh -c "cd /home/rocketmq/rocketmq-4.9.6 && sh bin/mqadmin topicRoute -n rmqnamesrv:9876 -t TopicTest"
```

如果 Topic 不存在，可以手动创建：

```powershell
docker exec rmqbroker sh -c "cd /home/rocketmq/rocketmq-4.9.6 && sh bin/mqadmin updateTopic -n rmqnamesrv:9876 -b 192.168.1.6:10911 -t TopicTest -r 4 -w 4"
```

### 3. 生产者 `SEND_OK`，但消费者没有打印消息

先区分两种情况：

1. Broker 里根本没有消息。
2. Broker 有消息，但消费者没有拉到或没有打印。

可以查看 Topic offset：

```powershell
docker exec rmqbroker sh -c "cd /home/rocketmq/rocketmq-4.9.6 && sh bin/mqadmin topicStatus -n rmqnamesrv:9876 -t TopicTest"
```

如果 `Max Offset` 增加，说明生产者已经写入 Broker。

再看消费者连接：

```powershell
docker exec rmqbroker sh -c "cd /home/rocketmq/rocketmq-4.9.6 && sh bin/mqadmin consumerConnection -n rmqnamesrv:9876 -g quickstart-consumer-group"
```

如果能看到 `TopicTest TagA`，说明消费者已经订阅成功。

本机遇到过一个 Broker 内部异常：

```text
NoClassDefFoundError: Could not initialize class org.apache.rocketmq.store.StoreUtil
```

这个异常会导致 Broker 拉消息失败。重建 Broker 时加入下面的环境变量后恢复：

```powershell
-e "JAVA_OPT_EXT=-XX:-UseContainerSupport"
```

### 4. 消费者组和消费位点

RocketMQ 会按消费者组记录消费进度。消费者组可以理解成“读者身份”，消费位点可以理解成“书签”。

例如 `quickstart-consumer-group` 已经读到第 10 条消息，那么下次继续使用这个消费者组时，它会从第 11 条以后开始读，不会自动从第 1 条重新读。

所以重复测试时，如果消费者没有打印旧消息，可以换一个新的消费者组：

```text
quickstart-consumer-group-test2
```

IDEA 中有两种方式：

1. 在 `QuickStartConsumer` 的 Run Configuration 里添加 VM options：

```text
-Drocketmq.consumerGroup=quickstart-consumer-group-test2
```

2. 或者直接临时修改 `QuickStartConstants.DEFAULT_CONSUMER_GROUP`。

修改后重新启动消费者，确认控制台中显示的是新消费者组：

```text
consumerGroup='quickstart-consumer-group-test2'
```

然后再运行生产者发送新消息。

### 5. 推荐测试顺序

最稳定的测试顺序：

```text
1. 启动 NameServer
2. 启动 Broker，并确认 Broker 是 Up
3. 确认 brokerIP1 是当前机器可访问的地址
4. 确认 TopicTest 存在
5. 启动消费者
6. 启动生产者发送新消息
7. 在消费者控制台查看消息打印
```

消费者成功时会看到类似：

```text
线程=ConsumeMessageThread_1, topic=TopicTest, tag=TagA, key=quickstart-key-0, queueId=2, queueOffset=0, body=Hello RocketMQ 0
```

## 注意点

- 启动顺序建议是：先启动 NameServer，再启动 Broker，再启动消费者，最后启动生产者。
- `TopicTest` 是文档示例里的主题名，学习环境可以使用；实际业务中应使用有业务含义的 Topic。
- 同一个消费者组会共享消费进度。重复测试时如果收不到旧消息，可以换一个新的消费者组。
- 文档中的 `RECONSUME_LATER` 适合演示失败重试，但真实业务不要无条件返回，否则同一条消息会不断重投。
- 如果 Java 客户端连接不上 Broker，优先检查第三章提到的 `brokerIP1`、防火墙、安全组和端口映射。

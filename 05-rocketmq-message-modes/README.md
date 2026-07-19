# 05 RocketMQ 消息模式

## 模块文件结构

```text
messagemode/
├── config/
│   ├── MessageModeConfig.java             # 读取 NameServer、Group、Topic 等运行配置
│   └── MessageScenario.java               # 定义各消息场景的 Topic 与默认 Tag 表达式
├── model/
│   └── OrderEvent.java                    # 顺序消息使用的订单流程事件
├── support/
│   ├── ConsumerLifecycle.java             # 长驻消费者生命周期管理
│   ├── MessageSupport.java                # 统一构建与打印消息
│   └── RocketMqClientFactory.java         # 集中创建 Producer、PushConsumer、PullConsumer
├── producer/
│   ├── BasicSendModeProducer.java         # 同步、异步、单向发送对比
│   ├── DelayMessageProducer.java          # 固定延迟等级消息
│   ├── OrderedMessageProducer.java        # 按订单号选择队列的分区顺序消息
│   ├── BatchMessageProducer.java          # 批量发送多条消息
│   ├── TransactionMessageProducer.java    # 事务提交、回滚与回查
│   └── FilterMessageProducer.java         # Tag 过滤和业务 Key
└── consumer/
    ├── PushMessageConsumer.java           # Push 模式并发消费者
    ├── PullMessageConsumer.java           # Lite Pull 主动拉取消费者
    └── OrderedMessageConsumer.java        # 顺序消息消费者
```

## 本章范围

`RocketMq.doc` 中章节边界不完全清晰。本模块按“消息是怎样发送、怎样投递、怎样分类”重新整理第 5 章：

1. 消费方式：Push、Pull。
2. 基础发送方式：同步、异步、单向。
3. 特殊消息：延迟、顺序、批量、事务。
4. 消息分类与定位：Tag、Key。

消费重试、死信消息和重复消费不放在本章，因为它们属于“消费失败治理与幂等性”，后续单独学习更容易建立完整知识链路。

## 总体流程

```text
Producer
  |
  |-- 同步：等待 Broker 返回 SendResult
  |-- 异步：立即返回，通过 SendCallback 接收结果
  |-- 单向：只发送，不等待 Broker 确认
  |-- 延迟：设置固定 delayTimeLevel
  |-- 顺序：相同业务键选择同一 MessageQueue
  |-- 批量：一次请求发送多条同 Topic 消息
  |-- 事务：半消息 -> 本地事务 -> Commit/Rollback/回查
  v
NameServer 路由 -> Broker -> Topic 下的 MessageQueue
  |
  |-- Push：客户端封装长轮询，消息到达后回调监听器
  |-- Pull：业务代码主动 poll，并控制拉取节奏
  v
Consumer
```

## 前置条件

先按第三章启动本地 RocketMQ：

- NameServer 默认地址：`127.0.0.1:9876`
- Broker 已成功注册到 NameServer
- Broker 开启 `autoCreateTopicEnable=true`，或者已经手动创建本章使用的 Topic
- 本机 Java 程序可以访问 Broker 注册到 NameServer 的 `brokerIP1`

本章默认使用六个 Topic，让不同消息类型、消费进度和排查结果彼此独立：

| 场景 | 默认 Topic |
| --- | --- |
| 普通消息 | `StudyNormalTopic` |
| 延迟消息 | `StudyDelayTopic` |
| 顺序消息 | `StudyOrderedTopic` |
| 批量消息 | `StudyBatchTopic` |
| 事务消息 | `StudyTransactionTopic` |
| Tag/Key 过滤 | `StudyFilterTopic` |

如果 Broker 未开启自动创建 Topic，可参考第三章的 `mqadmin updateTopic` 命令提前创建。

## 1. Push 与 Pull

这里的两种消费模式分别叫 **Push 模式**和 **Pull 模式**。`poll()` 是 Pull 消费者主动拉取消息时调用的方法，因此可以口头说“循环 poll”，但模式名称仍然是 Pull。

| 对比维度 | Push 模式 | Pull 模式 |
| --- | --- | --- |
| 示例 API | `DefaultMQPushConsumer` | `DefaultLitePullConsumer` |
| 表面行为 | 消息到达后自动回调监听器，看起来像 Broker 主动推送 | 业务代码主动调用 `poll()` 获取消息 |
| 实际机制 | 客户端内部通过长轮询从 Broker 拉取，再交给监听器 | 客户端同样从 Broker 拉取，但拉取循环由业务代码显式控制 |
| 消费入口 | `MessageListenerConcurrently` 或 `MessageListenerOrderly` | `consumer.poll(timeout)` 返回消息列表 |
| 拉取节奏 | RocketMQ 客户端自动维护 | 业务代码控制轮询频率、等待时间和每轮处理节奏 |
| 流量控制 | 主要使用客户端提供的线程数、批量大小和流控参数 | 可以在业务循环中暂停、限速或按自身处理能力决定何时再次拉取 |
| 实时性 | 通常较好，适合消息到达后尽快处理 | 取决于 `poll()` 频率，间隔过长会增加消费延迟 |
| 代码复杂度 | 较低，只需注册监听器并返回消费状态 | 较高，需要自己维护循环、空结果、异常、暂停和关闭逻辑 |
| 适用场景 | 普通业务事件、订单通知、库存处理等大多数实时消费场景 | 批处理、需要精细背压、按业务节奏拉取或需要主动控制消费窗口的场景 |
| 学习重点 | 理解监听器、消费状态以及 Push 底层仍然依赖长轮询 | 理解 `poll()`、主动流控以及消费循环的生命周期管理 |

### Push 模式

`DefaultMQPushConsumer` 看起来是 Broker 主动推送，客户端 API 实际仍通过长轮询拉取并封装了拉取、线程池和回调。大多数业务优先使用 Push 模式，代码简单且实时性好。

启动普通消息 Push 消费者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="normal"
```

### Pull 模式

`DefaultLitePullConsumer` 由业务代码主动调用 `poll`，可以自行控制每次拉取后的处理节奏，适合需要精细流控的场景。示例每次最多等待 1 秒，并在两轮拉取之间暂停 500 毫秒。

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PullMessageConsumer
```

Push 和 Pull 示例使用不同消费者组，因此两者都会收到普通 Topic 的消息。若希望它们共同分摊消息，需要显式配置成同一个消费者组。

## 2. 同步、异步与单向发送

`BasicSendModeProducer` 通过参数切换三种基础模式：

| 模式 | 调用 | 是否获得 Broker 结果 | 适合场景 |
| --- | --- | --- | --- |
| 同步 | `producer.send(message)` | 是，当前线程等待 | 订单、支付等重要消息 |
| 异步 | `producer.send(message, callback)` | 是，通过回调获得 | 对响应时间敏感且仍需确认结果 |
| 单向 | `producer.sendOneway(message)` | 否 | 可容忍丢失的日志或监控数据 |

```powershell
# 同步，省略参数时也是 sync
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.BasicSendModeProducer `
  -Dexec.args="sync"

# 异步
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.BasicSendModeProducer `
  -Dexec.args="async"

# 单向
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.BasicSendModeProducer `
  -Dexec.args="oneway"
```

异步示例使用 `CountDownLatch` 等待回调，避免主进程过早退出导致看不到发送结果。单向发送只代表客户端调用完成，不代表 Broker 已确认持久化。

## 3. 延迟消息

RocketMQ 4.x 不支持任意延迟时间，而是通过 `delayTimeLevel` 选择固定等级：

```text
1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
```

先启动延迟消息消费者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="delay"
```

再发送默认等级 3（约 10 秒）的延迟消息：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.DelayMessageProducer
```

也可以传入 1 至 18 的等级：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.DelayMessageProducer `
  -Dexec.args="4"
```

延迟时间不是实时调度 SLA，Broker 负载、存储和消费堆积都会影响最终到达时间。

## 4. 顺序消息

RocketMQ 默认把消息轮询发送到多个队列，跨队列没有全局顺序。本示例实现的是更常用的“分区有序”：

1. 生产者通过 `MessageQueueSelector` 对订单号取模。
2. 相同订单号始终进入同一个队列。
3. 消费者使用 `MessageListenerOrderly` 按队列顺序处理。
4. 订单 111 与订单 112 之间可以交错，但每个订单内部仍保持“下订单 -> 物流 -> 签收/拒收”。

先启动顺序消费者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.OrderedMessageConsumer
```

再启动顺序生产者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.OrderedMessageProducer
```

顺序保证的前提不仅是发送到同一队列，消费端也必须使用顺序监听器。若业务处理失败，应返回 `SUSPEND_CURRENT_QUEUE_A_MOMENT`，不能跳过失败消息继续消费同一队列。

## 5. 批量消息

批量发送用于减少网络调用次数，但需要满足以下约束：

- 同一批消息必须属于同一个 Topic。
- 不支持把延迟消息放进普通批次。
- 单批总大小不能超过客户端和 Broker 限制；较大集合应自行拆批，生产中通常预留协议开销，不要顶着 4 MiB 上限发送。

先启动批量场景消费者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="batch"
```

再发送三条消息组成的批次：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.BatchMessageProducer
```

需要注意：批量发送只是一次请求携带多条消息，它们仍是多条逻辑消息，各自拥有消息 ID。消费者监听器一次可能收到一条或多条，业务代码应遍历回调列表，不能只处理 `messages.get(0)`。

## 6. 事务消息

事务消息用于保证“本地事务结果”和“消息最终是否对消费者可见”一致，主要流程是：

```text
发送半消息
  |
  v
Broker 保存成功，但消费者暂不可见
  |
  v
Producer 执行本地事务
  |
  |-- COMMIT_MESSAGE   -> 消息对消费者可见
  |-- ROLLBACK_MESSAGE -> 消息被回滚
  `-- UNKNOW           -> Broker 稍后回查本地事务状态
```

先启动事务消息消费者：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="transaction"
```

分别观察三种状态：

```powershell
# 提交：消费者可以收到消息
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.TransactionMessageProducer `
  -Dexec.args="commit"

# 回滚：消费者收不到消息
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.TransactionMessageProducer `
  -Dexec.args="rollback"

# 首次未知：默认等待 70 秒，以便观察 Broker 回查后提交
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.TransactionMessageProducer `
  -Dexec.args="unknown 70"
```

示例用内存 Map 模拟本地事务记录，只用于理解回查流程。生产环境必须把事务状态保存在数据库等持久化介质中，并让 `checkLocalTransaction` 只查询事务结果，不要在回查方法里重复执行业务。

## 7. Tag 与 Key

- Topic 用来隔离不同业务或不同消息类型。
- Tag 用来区分同一业务 Topic 下的相关子类型，消费者可以通过 `TagA || TagB` 过滤。
- Key 是业务索引，例如订单号；可以在 Dashboard 或管理工具中定位消息，但不能代替业务幂等控制。

先启动过滤消费者。默认只订阅 `TagA || TagB`：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="filter"
```

再发送 TagA、TagB、TagC 三条消息：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.producer.FilterMessageProducer
```

消费者应只打印 TagA 和 TagB。若想订阅全部 Tag：

```powershell
mvn -q -pl 05-rocketmq-message-modes exec:java `
  -Dexec.mainClass=com.example.rocketmqstudy.messagemode.consumer.PushMessageConsumer `
  -Dexec.args="filter" `
  -Drocketmq.tagExpression="*"
```

## 配置项

配置优先级为 JVM 系统属性 > 环境变量 > 默认值。

| 配置 | JVM 系统属性 | 环境变量 | 默认值 |
| --- | --- | --- | --- |
| NameServer 地址 | `rocketmq.namesrvAddr` | `ROCKETMQ_NAMESRV_ADDR` | `127.0.0.1:9876` |
| 生产者组前缀 | `rocketmq.producerGroupPrefix` | `ROCKETMQ_PRODUCER_GROUP_PREFIX` | `message-mode-producer` |
| 消费者组前缀 | `rocketmq.consumerGroupPrefix` | `ROCKETMQ_CONSUMER_GROUP_PREFIX` | `message-mode-consumer` |
| 消费 Tag 表达式 | `rocketmq.tagExpression` | `ROCKETMQ_TAG_EXPRESSION` | 随场景变化 |

每个 Topic 都可以单独覆盖：

| 场景 | JVM 系统属性 | 环境变量 |
| --- | --- | --- |
| 普通 | `rocketmq.topic.normal` | `ROCKETMQ_TOPIC_NORMAL` |
| 延迟 | `rocketmq.topic.delay` | `ROCKETMQ_TOPIC_DELAY` |
| 顺序 | `rocketmq.topic.ordered` | `ROCKETMQ_TOPIC_ORDERED` |
| 批量 | `rocketmq.topic.batch` | `ROCKETMQ_TOPIC_BATCH` |
| 事务 | `rocketmq.topic.transaction` | `ROCKETMQ_TOPIC_TRANSACTION` |
| 过滤 | `rocketmq.topic.filter` | `ROCKETMQ_TOPIC_FILTER` |

## 编译

在项目根目录执行：

```powershell
mvn -q -pl 05-rocketmq-message-modes package
```

也可以聚合编译全部程序型章节：

```powershell
mvn -q clean package
```

## 推荐学习顺序

```text
1. 启动 PushMessageConsumer normal
2. 依次发送 sync、async、oneway，比较生产者日志
3. 启动 PullMessageConsumer，再发送普通消息，观察主动 poll
4. 测试 delay，比较生产时间和 bornTime/消费时间
5. 测试 ordered，按 orderNumber 检查同一订单的步骤顺序
6. 测试 batch，确认消费者遍历了回调中的全部消息
7. 测试 transaction 的 commit、rollback、unknown
8. 测试 filter，确认 TagC 被默认订阅表达式过滤，并记录 Key
```

## 本章复习问答

### 1. 同步、异步、单向发送是否能获得 Broker 确认？

| 发送方式 | 是否获得 Broker 确认 | 代码表现 | 主要特点 |
| --- | --- | --- | --- |
| 同步发送 | 是 | 当前线程等待 `SendResult` | 调用简单、结果明确，但等待期间会占用当前线程 |
| 异步发送 | 是 | 发送方法先返回，随后通过 `SendCallback` 获得成功或失败结果 | 不阻塞当前业务线程，但要正确处理回调、异常和生产者生命周期 |
| 单向发送 | 否 | 调用 `sendOneway()` 后没有 `SendResult` | 调用开销较低，但无法知道 Broker 是否成功接收，存在更高的消息丢失风险 |

这里的“Broker 确认”只表示 Broker 对本次发送给出了结果，不等于消费者已经完成业务处理。可以记成：**同步当场等结果，异步稍后回调结果，单向不关心结果。**

### 2. Push 为什么底层仍然是拉取？

`DefaultMQPushConsumer` 的使用体验像 Broker 主动推送：注册监听器后，消息到达就会触发回调。但实际网络交互仍由客户端主动向 Broker 发起长轮询请求：

```text
Consumer 发起拉取请求
  |
  |-- Broker 有消息：立即返回
  `-- Broker 暂无消息：保持请求一段时间，有消息或超时后再返回
  |
Consumer 收到结果并再次发起长轮询
```

RocketMQ 客户端把持续拉取、线程池、流量控制和监听器回调封装起来，所以业务代码感觉像“被推送”。可以记成：**Push 是使用体验，长轮询 Pull 是底层实现。**

### 3. 顺序消息为什么需要生产者和消费者共同配合？

RocketMQ 的顺序保证以 `MessageQueue` 为边界，不同队列之间没有天然的先后关系，因此两端都必须配合：

1. 生产者使用订单号等业务键选择队列，保证同一个订单的消息始终进入同一个 `MessageQueue`。
2. 消费者使用 `MessageListenerOrderly`，对同一个队列按顺序处理，不能让同一队列里的后续消息并发超越前一条消息。

如果生产者把同一订单发到不同队列，消费者无法恢复跨队列顺序；如果消费者并发处理同一队列，即使发送顺序正确，业务完成顺序也可能被打乱。本章实现的是“订单内有序”的分区顺序，不是整个 Topic 的全局顺序。可以记成：**生产者保证同一业务键进同一队列，消费者保证同一队列按顺序处理。**

### 4. 事务消息的半消息、提交、回滚、回查分别是什么？

- 半消息：Producer 先把消息发送给 Broker。Broker 已保存消息，但暂时不允许普通消费者看到。
- 提交 `COMMIT_MESSAGE`：本地事务成功，Broker 将半消息转为消费者可见的正式消息。
- 回滚 `ROLLBACK_MESSAGE`：本地事务失败，Broker 不再把这条消息投递给消费者。
- 未知 `UNKNOW`：Producer 暂时无法确定本地事务结果，Broker 稍后调用事务监听器进行回查。
- 事务回查：Producer 查询数据库中的本地事务记录，并再次返回提交、回滚或未知状态。回查方法应该查询已有结果，不能重新执行一次下单或扣款业务。

```text
发送半消息
  -> 执行本地事务
     -> 成功：Commit，消息可见
     -> 失败：Rollback，消息不投递
     -> 未知：Broker 回查本地事务状态
```

==事务消息保证的是“本地事务结果”和“消息是否提交”的最终一致性，不代表消费者业务只会执行一次，消费端仍然需要幂等设计。==可以记成：**先藏住消息，再根据本地事务结果决定公开、丢弃还是回查。**

### 5. Topic、Tag、Key 分别解决什么问题？

| 概念 | 主要作用 | 示例 | 注意点 |
| --- | --- | --- | --- |
| Topic | 隔离不同业务领域或不同消息类型，也是消息路由和队列组织的基本单位 | 普通消息、顺序消息和事务消息使用不同 Topic | 没有直接关联或处理要求不同的消息应使用不同 Topic |
| Tag | 在同一个 Topic 内划分相关的消息子类型，供消费者进行订阅过滤 | `TagA || TagB` | Tag 适合同一业务集合中的子分类，不能代替 Topic 的业务隔离 |
| Key | 保存订单号等业务标识，便于通过控制台或管理工具查询、定位消息 | `order-2001` | Key 不会自动保证唯一，也不能直接解决重复消费和业务幂等问题 |

可以记成：**Topic 分业务，Tag 分子类，Key 找消息。**

## 注意点

- 同一消费者组会共享消费位点。重复学习旧消息时，可修改 `rocketmq.consumerGroupPrefix` 使用新组。
- 新消费者组配合 `CONSUME_FROM_FIRST_OFFSET` 可以从该组尚未消费的位置开始，但不会绕过消息保留期限。
- 顺序消息只保证同一个队列内有序，不要把“订单内有序”误解成整个 Topic 全局有序。==（相同订单进入同一队列，所以订单内部有序；不同订单可能进入不同队列并行处理）==
- 异步回调和事务回查运行在客户端线程中，业务处理要线程安全。
- 事务消息解决的是本地事务与消息提交的一致性，消费者仍应做好幂等处理。
- 本章刻意不演示无限重试；失败重试、死信和重复消费应在后续章节结合幂等方案一起学习。

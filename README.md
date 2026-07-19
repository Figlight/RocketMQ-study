# RocketMQ 学习示例

本项目根据 `RocketMq.doc` 的目录逐章整理学习内容。每个知识点使用一个独立文件夹承载：

1. `README.md`：说明本章概念、学习目标、注意点和运行方式。
2. 程序型章节使用 `src/main/java` 存放本章对应的可运行 Java 示例。
3. 程序型章节提供 `pom.xml`，保持章节示例可以独立编译，也可以通过根目录 Maven 聚合编译。
4. 流程型章节只提供文档，例如 `RocketMQ安装` 只写安装步骤和注意点。

## Demo 代码结构约定

- `main` 类只负责串联演示流程，不把所有类、接口都写成内部类。
- 按职责拆分包，例如 `model`、`service`、`consumer`、`producer`、`broker`、`nameserver`、`mq`、`support`。
- 文件规模变大时，继续按业务关联拆分子包，避免单文件过长。
- 每个类、方法、字段都补充注释，方便学习时直接阅读源码。
- 每个章节 README 都要包含模块文件结构、流程图、章节介绍、示例说明、运行方式和注意点。

## 当前章节

| 目录 | 对应文档章节 | 内容 |
| --- | --- | --- |
| `01-rocketmq-introduction` | RocketMQ 简介 | 用内存队列模拟 MQ 的异步、解耦、削峰思想 |
| `02-rocketmq-concepts` | RocketMQ 概念 | 用内存模型模拟 Producer、Consumer、Broker、NameServer、Topic、Queue、Group |
| `03-rocketmq-installation` | RocketMQ 安装 | RocketMQ 本地 Docker 安装与 Linux 压缩包安装流程 |
| `04-rocketmq-quickstart` | RocketMQ 快速入门 | 使用真实 RocketMQ 客户端演示同步发送和 PushConsumer 消费 |
| `05-rocketmq-message-modes` | RocketMQ 消息模式 | 演示 Push/Pull、同步/异步/单向、延迟、顺序、批量、事务以及 Tag/Key |
| `06-rocketmq-retry-and-dlq` | RocketMQ 重试机制与死信消息 | 演示发送重试、消费重试、异常分类、死信队列与死信消费 |
| `07-rocketmq-duplicate-consumption-and-idempotency` | RocketMQ 消息重复消费问题 | 演示重复投递风险、稳定业务 Key 与消费幂等处理 |
| `08-rocketmq-spring-boot-integration` | RocketMQ 集成 Spring Boot | 使用 RocketMQTemplate、注解监听器和 REST 接口演示 Spring Boot 消息收发 |

## 后续约定

- 每遇到一个新的知识点，就新建一个独立文件夹。
- 概念章节优先使用不依赖外部服务的最小可运行程序帮助理解。
- 需要真实 RocketMQ 服务的章节，会在示例说明中写清楚启动前置条件。
- 类似 `RocketMQ安装` 这种流程型章节，只编写 `.md` 流程说明，不额外编写程序。

## 编译全部示例

安装章节是纯文档，不参与 Maven 编译。程序型章节可在根目录执行：

```bash
mvn clean package
```

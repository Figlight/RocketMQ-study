# 03 RocketMQ 安装

## 模块文件结构

```text
03-rocketmq-installation/
├── README.md                         # RocketMQ 安装流程、启动验证、常见问题
└── docker/
    └── broker.conf                   # 本地 Docker 学习用 Broker 配置
```

## 流程图

```text
准备环境
  |
  |-- Docker 本地学习（推荐）
  |      |
  |      |-- 拉取 apache/rocketmq 镜像
  |      |-- 创建 rocketmq Docker 网络
  |      |-- 启动 NameServer，暴露 9876
  |      |-- 编写 broker.conf
  |      |-- 启动 Broker，连接 NameServer
  |      |-- 使用 Producer / Consumer 工具验证收发
  |      |
  |      v
  |   本机 Java 示例可连接 127.0.0.1:9876
  |
  |-- Linux 压缩包安装（贴近课程文档）
         |
         |-- 下载 rocketmq-all-4.9.2-bin-release.zip
         |-- 解压到服务器目录
         |-- 配置 NAMESRV_ADDR
         |-- 调小 runserver.sh / runbroker.sh 内存参数
         |-- 修改 conf/broker.conf
         |-- 启动 mqnamesrv
         |-- 启动 mqbroker
         |-- 使用 tools.sh 验证收发
         |
         v
      服务器 Java 客户端可连接 服务器IP:9876
```

## 本章对应文档内容

文档第三章主要说明：

- 下载 RocketMQ。
- 上传服务器并解压。
- 修改 NameServer 和 Broker 的启动脚本内存参数。
- 配置 `broker.conf`。
- 启动 NameServer、Broker。
- 安装 RocketMQ Dashboard 控制台。
- 使用 Docker 启动 NameServer、Broker、控制台。

## 本章先理解要安装什么

安装 RocketMQ 时不要只盯着命令，先把组件关系理顺：

```text
+------------------+      注册 Broker 信息      +------------------+
| Broker           | -------------------------> | NameServer       |
| 保存和投递消息    |                            | 保存路由信息      |
+------------------+                            +------------------+
         ^                                                ^
         |                                                |
         | 写入消息                                       | 查询路由
         |                                                |
+------------------+                            +------------------+
| Producer         |                            | Consumer         |
| 发送消息          |                            | 消费消息          |
+------------------+                            +------------------+
```

必须启动的组件只有两个：

| 组件 | 是否必须 | 作用 |
| --- | --- | --- |
| NameServer | 必须 | 保存 Broker 路由，Producer 和 Consumer 都要先找它 |
| Broker | 必须 | 真正保存消息、投递消息 |
| Dashboard | 可选 | 可视化查看 Topic、消息、消费组，学习时很有帮助 |

因此后续 Java 示例真正依赖的是 `NameServer + Broker`，Dashboard 只是辅助观察工具。

## 推荐安装方式

如果你只是本地学习，建议优先使用 Docker 安装。原因是：

- 不需要手动解压 RocketMQ 压缩包。
- 不需要改 `runserver.sh`、`runbroker.sh`。
- 启动和清理都比较方便。
- Windows 本地学习时，比直接跑 Linux 脚本更省心。

课程文档使用的是 RocketMQ `4.9.2` 压缩包；官方 4.x Docker 快速开始文档示例使用的是 `apache/rocketmq:4.9.6`。学习本项目后续 Java 客户端示例时，优先保持 RocketMQ 服务端和客户端在 `4.9.x` 系列即可。

### 和原文档 Docker 镜像的差异

原文档里的 Docker 命令使用：

```bash
docker pull rocketmqinc/rocketmq
docker pull styletang/rocketmq-console-ng
```

这套写法来自较早期课程资料，可以作为“旧版命令对照”理解，但不建议作为现在新安装的首选：

- `rocketmqinc/rocketmq` 是旧镜像路线，课程中还出现了 `/opt/rocketmq-4.4.0/...` 这样的旧容器目录。
- `styletang/rocketmq-console-ng` 是早期控制台镜像，后续 RocketMQ 控制台已经演进为 `rocketmq-dashboard`。
- 官方 4.x Docker 快速开始当前示例使用 `apache/rocketmq:4.9.6`，目录、命令和后续 Java 客户端版本更容易统一。

所以本项目采用的原则是：

| 场景 | 建议以哪个为准 |
| --- | --- |
| Windows + Docker Desktop 本地学习 | 以本章 `apache/rocketmq:4.9.6` 为准 |
| 严格复刻原课程截图 | 才参考 `rocketmqinc/rocketmq` 旧命令 |
| 后续 Java 客户端示例 | 优先使用 `4.9.x` 系列，服务端和客户端版本保持接近 |
| 控制台 Dashboard | 优先使用 Apache RocketMQ Dashboard，不优先使用旧 `console-ng` |

## 方式一：Docker 本地安装（推荐）

### 前置条件

需要提前安装：

- Docker Desktop。
- 64 位操作系统。
- JDK 1.8+，后续 Java 示例需要用到。

### 1. 拉取 RocketMQ 镜像

```bash
docker pull apache/rocketmq:4.9.6
```

### 2. 创建 Docker 网络

RocketMQ 至少包含 NameServer 和 Broker 两类服务，创建独立网络可以让容器之间用容器名通信。

```bash
docker network create rocketmq
```

### 3. 启动 NameServer

```bash
docker run -d --name rmqnamesrv -p 9876:9876 --net rocketmq apache/rocketmq:4.9.6 sh mqnamesrv
```

查看启动日志：

```bash
docker logs -f rmqnamesrv
```

看到类似下面的日志，说明 NameServer 启动成功：

```text
The Name Server boot success...
```

### 4. 确认 Broker 配置文件

本项目已经提供学习阶段使用的 Broker 配置文件：

```text
03-rocketmq-installation/docker/broker.conf
```

内容如下：

```properties
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
brokerIP1=host.docker.internal
autoCreateTopicEnable=true
```

参数说明：

- `brokerClusterName`：Broker 所属集群名。
- `brokerName`：Broker 名称。
- `brokerId`：`0` 表示 Master。
- `deleteWhen`：消息清理时间，默认凌晨 4 点。
- `fileReservedTime`：消息文件保留时间，单位小时。
- `brokerRole`：Broker 角色，学习环境用 `ASYNC_MASTER` 即可。
- `flushDiskType`：刷盘方式，学习环境用 `ASYNC_FLUSH` 即可。
- `brokerIP1`：Broker 对外暴露的地址。Docker Desktop 本地学习写 `host.docker.internal`，这样 Dashboard 容器和宿主机 Java 程序都能通过 Docker 端口转发访问 Broker。
- `autoCreateTopicEnable`：允许自动创建 Topic，学习阶段可以打开。

后续如果部署到云服务器，`brokerIP1` 要改成客户端能访问到的服务器 IP；如果只是本机 Docker Desktop 学习，保持 `host.docker.internal` 即可。

### 5. 启动 Broker

下面的命令建议在项目根目录执行，也就是 `D:\vibe-coding-project\RocketMQ-study`。

Windows PowerShell：

```powershell
docker run -d `
  --name rmqbroker `
  --net rocketmq `
  -p 10912:10912 -p 10911:10911 -p 10909:10909 `
  -e "NAMESRV_ADDR=rmqnamesrv:9876" `
  -v ${PWD}\03-rocketmq-installation\docker\broker.conf:/home/rocketmq/rocketmq-4.9.6/conf/broker.conf `
  apache/rocketmq:4.9.6 sh mqbroker `
  -c /home/rocketmq/rocketmq-4.9.6/conf/broker.conf
```

Linux / macOS：

```bash
docker run -d \
  --name rmqbroker \
  --net rocketmq \
  -p 10912:10912 -p 10911:10911 -p 10909:10909 \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  -v ./03-rocketmq-installation/docker/broker.conf:/home/rocketmq/rocketmq-4.9.6/conf/broker.conf \
  apache/rocketmq:4.9.6 sh mqbroker \
  -c /home/rocketmq/rocketmq-4.9.6/conf/broker.conf
```

查看启动日志：

```bash
docker logs -f rmqbroker
```

看到类似下面的日志，说明 Broker 启动成功：

```text
The broker boot success...
```

### 6. 使用官方工具验证收发消息

进入 Broker 容器：

```bash
docker exec -it rmqbroker bash
```

发送消息：

```bash
sh tools.sh org.apache.rocketmq.example.quickstart.Producer
```

消费消息：

```bash
sh tools.sh org.apache.rocketmq.example.quickstart.Consumer
```

如果看到 `SEND_OK` 和 `Receive New Messages`，说明 RocketMQ 已经可以正常收发消息。

### 7. 停止和清理容器

只停止：

```bash
docker stop rmqbroker rmqnamesrv
```

重新启动：

```bash
docker start rmqnamesrv rmqbroker
```

彻底删除：

```bash
docker rm -f rmqbroker rmqnamesrv
docker network rm rocketmq
```

## 方式二：Linux 压缩包安装（贴近课程文档）

### 1. 下载 RocketMQ 4.9.2

课程文档使用 `4.9.2`：

```bash
wget https://archive.apache.org/dist/rocketmq/4.9.2/rocketmq-all-4.9.2-bin-release.zip
```

注意：`archive.apache.org` 是 Apache 历史归档地址，适合跟随旧课程版本学习；正式生产环境应优先查看 Apache 当前下载页并选择受支持版本。

### 2. 解压

```bash
mkdir -p /root/rocketmq
mv rocketmq-all-4.9.2-bin-release.zip /root/rocketmq/
cd /root/rocketmq
unzip rocketmq-all-4.9.2-bin-release.zip
cd rocketmq-all-4.9.2-bin-release
```

如果没有 `unzip`：

```bash
yum install -y unzip
```

### 3. 配置环境变量

编辑 `/etc/profile`：

```bash
vim /etc/profile
```

追加：

```bash
export NAMESRV_ADDR=127.0.0.1:9876
```

使配置生效：

```bash
source /etc/profile
```

如果是在云服务器上给本机 Java 程序连接，`127.0.0.1` 要改成服务器公网 IP。

### 4. 调小 JVM 内存参数

学习环境通常内存不大，需要调小 RocketMQ 默认 JVM 参数。

编辑 NameServer 启动脚本：

```bash
vim bin/runserver.sh
```

把 `JAVA_OPT` 中的 `-Xms`、`-Xmx`、`-Xmn` 改小，例如：

```text
-Xms256m -Xmx256m -Xmn128m
```

编辑 Broker 启动脚本：

```bash
vim bin/runbroker.sh
```

把 `JAVA_OPT` 中的内存参数改小，例如：

```text
-Xms512m -Xmx512m -Xmn256m
```

### 5. 修改 Broker 配置

编辑：

```bash
vim conf/broker.conf
```

建议学习环境追加：

```properties
namesrvAddr=127.0.0.1:9876
autoCreateTopicEnable=true
brokerIP1=127.0.0.1
```

如果是在云服务器上，需要把 `brokerIP1` 改成服务器公网 IP，否则本地 Java 客户端可能拿到内网地址，导致连接失败。

### 6. 启动 NameServer 和 Broker

创建日志目录：

```bash
mkdir -p logs
```

启动 NameServer：

```bash
nohup sh bin/mqnamesrv > ./logs/namesrv.log 2>&1 &
```

启动 Broker：

```bash
nohup sh bin/mqbroker -c conf/broker.conf > ./logs/broker.log 2>&1 &
```

查看日志：

```bash
tail -f ./logs/namesrv.log
tail -f ./logs/broker.log
```

NameServer 看到 `The Name Server boot success...`，Broker 看到 `The broker[...] boot success...`，表示启动成功。

### 7. 验证消息收发

```bash
export NAMESRV_ADDR=127.0.0.1:9876
sh bin/tools.sh org.apache.rocketmq.example.quickstart.Producer
sh bin/tools.sh org.apache.rocketmq.example.quickstart.Consumer
```

### 8. 关闭服务

```bash
sh bin/mqshutdown broker
sh bin/mqshutdown namesrv
```

## Dashboard 控制台

控制台不是 RocketMQ 必需组件，但学习阶段建议安装，方便查看 Topic、消息和消费组。

### 方式一：源码打包

```bash
git clone https://github.com/apache/rocketmq-dashboard.git
cd rocketmq-dashboard
mvn clean package -Dmaven.test.skip=true
```

打包完成后运行生成的 jar：

```bash
java -jar target/rocketmq-dashboard-*.jar --rocketmq.config.namesrvAddr=127.0.0.1:9876 --server.port=8080
```

访问：

```text
http://localhost:8080
```

### 方式二：Docker 控制台

如果你只是本地学习，也可以后续再装 Dashboard。前几章 Java 示例只需要 NameServer 和 Broker 正常启动即可。

当前更推荐使用 Apache RocketMQ Dashboard 镜像：

```bash
docker pull apacherocketmq/rocketmq-dashboard:latest
docker run -d --name rocketmq-dashboard --net rocketmq -e "JAVA_OPTS=-Drocketmq.namesrv.addr=rmqnamesrv:9876" -p 8082:8082 apacherocketmq/rocketmq-dashboard:latest
```

访问：

```text
http://localhost:8082
```

如果 Dashboard 页面右上角出现 `connect to 127.0.0.1:10909 failed`，通常是 Broker 配置中的 `brokerIP1` 仍然是 `127.0.0.1`。Docker 容器里的 `127.0.0.1` 代表容器自己，不代表宿主机。请确认 `03-rocketmq-installation/docker/broker.conf` 中配置为：

```properties
brokerIP1=host.docker.internal
```

修改后重启 Broker 和 Dashboard：

```powershell
docker restart rmqbroker rocketmq-dashboard
```

## 日常启动、停止、重启

这一节是平时学习最常用的命令。第一次安装完成后，后面一般不需要重复 `docker run` 创建容器，只需要 `stop` 和 `start`。

### 1. 结束学习时停止 RocketMQ

平时学完以后，直接停止 Broker 和 NameServer 即可：

```powershell
docker stop rmqbroker rmqnamesrv
```

如果你也启动了 Dashboard，再停止 Dashboard：

```powershell
docker stop rocketmq-dashboard
```

说明：

- `docker stop` 只是停止容器，不会删除容器。
- 下次继续学习时，不需要重新拉镜像，也不需要重新执行 `docker run`。
- 建议先停 Broker，再停 NameServer，因为 Broker 依赖 NameServer 做路由注册。

### 2. 下次继续学习时重新启动

重新启动时，建议先启动 NameServer，再启动 Broker：

```powershell
docker start rmqnamesrv rmqbroker
```

如果你使用 Dashboard：

```powershell
docker start rocketmq-dashboard
```

### 3. 查看容器是否运行

```powershell
docker ps
```

重点看这几个容器是否在列表中：

```text
rmqnamesrv
rmqbroker
rocketmq-dashboard
```

如果只想看所有容器，包括已经停止的容器：

```powershell
docker ps -a
```

### 4. 查看启动日志

NameServer 日志：

```powershell
docker logs -f rmqnamesrv
```

看到下面内容，说明 NameServer 启动成功：

```text
The Name Server boot success. serializeType=JSON
```

Broker 日志：

```powershell
docker logs -f rmqbroker
```

看到类似下面内容，说明 Broker 启动成功：

```text
The broker[broker-a, ...] boot success...
```

退出日志跟随模式时，按：

```text
Ctrl + C
```

### 5. 验证 RocketMQ 是否还能收发消息

进入 Broker 容器：

```powershell
docker exec -it rmqbroker bash
```

发送测试消息：

```bash
sh tools.sh org.apache.rocketmq.example.quickstart.Producer
```

看到 `sendStatus=SEND_OK` 表示发送成功。

消费测试消息：

```bash
sh tools.sh org.apache.rocketmq.example.quickstart.Consumer
```

看到 `Consumer Started.` 表示消费者启动成功；继续收到 `Receive New Messages` 表示消费成功。

### 6. 什么时候才需要删除容器

平时不要用删除命令。只有在你想重装、配置改乱了、或者想彻底清理环境时，才执行：

```powershell
docker rm -f rmqbroker rmqnamesrv rocketmq-dashboard
docker network rm rocketmq
```

删除后，容器就不存在了。下次需要重新执行 `docker run` 创建 NameServer、Broker 和 Dashboard。

## 常见问题

### 1. 为什么要开放这些端口？

- `9876`：NameServer 端口。
- `10911`：Broker 主通信端口。
- `10909`：Broker VIP 通道相关端口。
- `10912`：HA 相关端口。

### 2. 本机能启动，Java 程序却连不上？

优先检查：

- `brokerIP1` 是否写成了客户端可访问的 IP。
- 防火墙或云服务器安全组是否开放 `9876`、`10911`、`10909`。
- Java 程序里的 `namesrvAddr` 是否和实际地址一致。

### 3. Docker Broker 启动后马上退出？

优先检查：

- `broker.conf` 是否挂载成功。
- Docker Desktop 是否有足够内存。
- `docker logs rmqbroker` 中是否提示找不到配置文件。

### 4. 学习时必须安装 Dashboard 吗？

不是必须。Dashboard 只是可视化工具；后续 Java 示例真正依赖的是 NameServer 和 Broker。

## 参考资料

- [Apache RocketMQ 4.x 本地快速开始](https://rocketmq.apache.org/docs/4.x/quickstart/01quickstart/)
- [Apache RocketMQ 4.x Docker 快速开始](https://rocketmq.apache.org/docs/4.x/quickstart/02quickstartWithDocker)
- [Apache RocketMQ 4.9.2 历史归档](https://archive.apache.org/dist/rocketmq/4.9.2/)
- [Apache RocketMQ Dashboard](https://github.com/apache/rocketmq-dashboard)

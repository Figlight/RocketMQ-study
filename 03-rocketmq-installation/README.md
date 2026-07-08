# 03 RocketMQ 安装

## 模块文件结构

```text
03-rocketmq-installation/
└── README.md                         # RocketMQ 安装流程、启动验证、常见问题
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

## 推荐安装方式

如果你只是本地学习，建议优先使用 Docker 安装。原因是：

- 不需要手动解压 RocketMQ 压缩包。
- 不需要改 `runserver.sh`、`runbroker.sh`。
- 启动和清理都比较方便。
- Windows 本地学习时，比直接跑 Linux 脚本更省心。

课程文档使用的是 RocketMQ `4.9.2` 压缩包；官方 4.x Docker 快速开始文档示例使用的是 `apache/rocketmq:4.9.6`。学习本项目后续 Java 客户端示例时，优先保持 RocketMQ 服务端和客户端在 `4.9.x` 系列即可。

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

### 4. 创建 Broker 配置文件

在任意学习目录下新建 `broker.conf`：

```properties
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
brokerIP1=127.0.0.1
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
- `brokerIP1`：Broker 对外暴露的地址，本机 Docker 学习写 `127.0.0.1`。
- `autoCreateTopicEnable`：允许自动创建 Topic，学习阶段可以打开。

### 5. 启动 Broker

Windows PowerShell：

```powershell
docker run -d `
  --name rmqbroker `
  --net rocketmq `
  -p 10912:10912 -p 10911:10911 -p 10909:10909 `
  -e "NAMESRV_ADDR=rmqnamesrv:9876" `
  -v ${PWD}\broker.conf:/home/rocketmq/rocketmq-4.9.6/conf/broker.conf `
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
  -v ./broker.conf:/home/rocketmq/rocketmq-4.9.6/conf/broker.conf \
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

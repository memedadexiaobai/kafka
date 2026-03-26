⏺ Kafka 在没有 Zookeeper 下启动（KRaft 模式）

Kafka 从 2.8 版本引入了 KRaft 模式（Kafka Raft Metadata mode），可以完全脱离 Zookeeper 独立运行。

核心判断逻辑

启动时根据 process.roles 配置判断使用哪种模式：

// core/src/main/scala/kafka/Kafka.scala
private def buildServer(props: Properties): Server = {
val config = KafkaConfig.fromProps(props, doLog = false)
if (config.requiresZookeeper) {
// Zookeeper 模式（process.roles 为空）
new KafkaServer(config, Time.SYSTEM, ...)
} else {
// KRaft 模式（process.roles 非空）
new KafkaRaftServer(config, Time.SYSTEM)
}
}

// KafkaConfig.scala
def requiresZookeeper: Boolean = processRoles.isEmpty
def usesSelfManagedQuorum: Boolean = processRoles.nonEmpty

  ---
配置方式

1. 组合模式（单节点既是 broker 又是 controller）

# 核心配置
process.roles=broker,controller
node.id=1
metadata.log.dir=/tmp/kraft-metadata
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
advertised.listeners=PLAINTEXT://localhost:9092

# 仲裁配置
quorum.voters=1@localhost:9093

# 日志目录
log.dirs=/tmp/kafka-logs

2. 分离模式（controller 和 broker 分离）

Controller 节点配置：

process.roles=controller
node.id=1
metadata.log.dir=/tmp/kraft-metadata
listeners=CONTROLLER://:9093
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095

Broker 节点配置：

process.roles=broker
node.id=2
listeners=PLAINTEXT://:9092
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
log.dirs=/tmp/kafka-logs

3. 多节点集群示例

节点 1：

process.roles=broker,controller
node.id=1
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
metadata.log.dir=/tmp/kraft-metadata-1
log.dirs=/tmp/kafka-logs-1

节点 2：

process.roles=broker,controller
node.id=2
listeners=PLAINTEXT://:9094,CONTROLLER://:9095
controller.listener.names=CONTROLLER
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
metadata.log.dir=/tmp/kraft-metadata-2
log.dirs=/tmp/kafka-logs-2

节点 3：

process.roles=broker,controller
node.id=3
listeners=PLAINTEXT://:9096,CONTROLLER://:9097
controller.listener.names=CONTROLLER
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
metadata.log.dir=/tmp/kraft-metadata-3
log.dirs=/tmp/kafka-logs-3

  ---
启动命令

# 使用配置文件启动
bin/kafka-server-start.sh config/kraft/server.properties

# 后台运行
bin/kafka-server-start.sh -daemon config/kraft/server.properties

  ---
初始化元数据存储（仅首次运行）

如果 metadata.log.dir 不包含已有的元数据，需要先初始化：

# 格式化存储目录
bin/kafka-storage.sh format -t <cluster-id> -c config/kraft/server.properties

# 示例
bin/kafka-storage.sh format -t _b1d-2a1l-3x4y -c config/kraft/server.properties

  ---
核心配置参数说明
┌───────────────────────────┬─────────────────────────────────────┬──────────────────────────────┐
│           参数            │                说明                 │           是否必需           │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ process.roles             │ 进程角色：broker、controller 或两者 │        KRaft 模式必需        │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ node.id                   │ 节点唯一标识                        │        KRaft 模式必需        │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ metadata.log.dir          │ 元数据日志存储目录                  │        KRaft 模式必需        │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ quorum.voters             │ Controller 仲裁投票者列表           │        KRaft 模式必需        │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ controller.listener.names │ Controller 使用的监听器名称         │ 当角色包含 controller 时必需 │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ listeners                 │ 监听器列表                          │             必需             │
├───────────────────────────┼─────────────────────────────────────┼──────────────────────────────┤
│ log.dirs                  │ 数据日志目录                        │       broker 角色必需        │
└───────────────────────────┴─────────────────────────────────────┴──────────────────────────────┘
  ---
关键代码文件
┌────────────────────────────────────────┬────────────────────────────────────┐
│                  文件                  │                作用                │
├────────────────────────────────────────┼────────────────────────────────────┤
│ kafka/Kafka.scala                      │ 启动入口，判断使用哪种模式         │
├────────────────────────────────────────┼────────────────────────────────────┤
│ kafka/server/KafkaRaftServer.scala     │ KRaft 模式服务器实现               │
├────────────────────────────────────────┼────────────────────────────────────┤
│ kafka/server/KafkaServer.scala         │ Zookeeper 模式服务器实现           │
├────────────────────────────────────────┼────────────────────────────────────┤
│ kafka/server/KafkaConfig.scala         │ 配置类，包含 processRoles 解析逻辑 │
├────────────────────────────────────────┼────────────────────────────────────┤
│ kafka/server/config/KRaftConfigs.scala │ KRaft 专用配置定义                 │
└────────────────────────────────────────┴────────────────────────────────────┘
  ---
迁移模式（ZK → KRaft）

Kafka 还支持从 Zookeeper 模式平滑迁移到 KRaft 模式：

# 在 ZK broker 上启用迁移
process.roles=broker
migration.enabled=true
node.id=2
quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
zookeeper.connect=localhost:2181  # 仍然连接 ZK

这种模式下，ZK broker 可以与 KRaft controller 通信，逐步完成迁移。
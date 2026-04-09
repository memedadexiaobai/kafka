在 Kafka 中，`listenerName` 用于标识不同的监听器（Listener），每个监听器可以配置不同的网络接口和端口，以便接收来自不同客户端或 Kafka 节点的请求。常见的 `listenerName` 命名规范和相关配置如下：

### 常见的 `listenerName` 命名规范：
- **PLAINTEXT**：表示不加密的普通文本通信。
- **SSL**：表示使用 SSL 加密的通信。
- **SASL_PLAINTEXT**：表示使用 SASL 认证的非加密通信。
- **SASL_SSL**：表示使用 SASL 认证和 SSL 加密的通信。
- **CLIENT**：用于接收客户端流量的监听器。
- **BROKER**：用于 Broker 之间通信的监听器。
- **CONTROLLER**：用于 Controller 通信的监听器。
- **EXTERNAL**：用于外部客户端连接的监听器。
- **INTERNAL**：用于内部客户端连接的监听器。

### 完整的相关配置：
1. **listeners**：指定 Kafka Broker 监听的网络接口和端口。格式为：`{LISTENER_NAME}://{hostname}:{port}`。例如：
   ```
   listeners=CLIENT://localhost:9092,BROKER://localhost:9093
   ```
   这表示 Broker 监听两个端口，分别用于客户端通信和 Broker 之间通信。

2. **advertised.listeners**：向客户端发布的 Broker 连接地址。例如：
   ```
   advertised.listeners=CLIENT://your.host.name:9092,BROKER://your.host.name:9093
   ```
   客户端通过这个地址连接到 Broker。

3. **listener.security.protocol.map**：将监听器名称映射到安全协议。例如：
   ```
   listener.security.protocol.map=CLIENT:SSL,BROKER:SASL_SSL
   ```
   这表示 CLIENT 监听器使用 SSL 协议，BROKER 监听器使用 SASL_SSL 协议。

4. **inter.broker.listener.name**：指定 Broker 之间通信的监听器名称。例如：
   ```
   inter.broker.listener.name=BROKER
   ```
   Broker 之间通过这个监听器进行通信。

5. **control.plane.listener.name**：用于 Controller 和 Broker 之间通信的监听器名称。例如：
   ```
   control.plane.listener.name=CONTROLLER
   ```
   Controller 使用这个监听器与 Broker 通信。

6. **controller.listener.names**：在 KRaft 模式下，指定 Controller 的监听器名称。例如：
   ```
   controller.listener.names=CONTROLLER
   ```
   Controller 使用这个监听器与其他 Controller 和 Broker 通信。

7. **security.inter.broker.protocol**：指定 Broker 之间通信的安全协议。如果未配置 `inter.broker.listener.name`，则使用该配置。例如：
   ```
   security.inter.broker.protocol=SASL_SSL
   ```
   Broker 之间通过这个协议进行通信。

### 注意事项：
- **listeners 和 advertised.listeners 的区别**：`listeners` 用于定义 Broker 监听的网络接口和端口，而 `advertised.listeners` 用于向客户端发布 Broker 的连接地址。
- **hostname 的设置**：如果 `listeners` 中的 hostname 为空，表示绑定到所有接口（0.0.0.0）。如果设置为 0.0.0.0，必须配置 `advertised.listeners`，以便客户端能够正确连接。
- **listenerName 的唯一性**：监听器名称和端口必须唯一，不能有两个监听器具有相同的名称。

这些配置项共同确保了 Kafka 集群在网络通信中的安全性和灵活性。
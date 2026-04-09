# JAAS (Java Authentication and Authorization Service) 详解

## 🎯 **什么是 JAAS？**

**JAAS** = **Java Authentication and Authorization Service**（Java 认证和授权服务）

这是 Java 提供的一套**安全框架**，用于：
1. **认证 (Authentication)**：验证用户/服务的身份（你是谁？）
2. **授权 (Authorization)**：控制已认证主体的访问权限（你能做什么？）

---

## 📚 **JAAS 核心概念**

### **1. Subject（主体）**
```java
// 代表一个用户或服务实体
Subject {
    Principals: [用户身份标识]
    PublicCredentials: [公开凭证，如用户名]
    PrivateCredentials: [私有凭证，如密码、密钥]
}
```


**示例：**
```java
// Kafka Server 的 Subject
Subject {
    Principals: ["KafkaBroker-1"]
    PublicCredentials: ["admin"]
    PrivateCredentials: ["secret_password"]
}
```


---

### **2. Principal（主体标识）**
表示 Subject 的一个身份标识。

**示例：**
```java
// Kerberos Principal
"admin@EXAMPLE.COM"

// SSL Certificate Principal  
"CN=kafka-server, OU=IT, O=MyCompany, L=Beijing, ST=BJ, C=CN"

// Simple Principal
"username=admin"
```


---

### **3. LoginModule（登录模块）** ⭐

这是 JAAS 的核心，负责实际的认证逻辑。

**标准接口：**
```java
public interface LoginModule {
    boolean login();           // 执行登录
    boolean commit();          // 提交认证结果
    boolean abort();           // 回滚认证
    boolean logout();          // 登出
    void initialize(...);      // 初始化配置
}
```


**Kafka 常用的 LoginModule：**

| LoginModule | 用途 | 配置示例 |
|-------------|------|---------|
| `PlainLoginModule` | 简单用户名密码 | `username="admin" password="secret"` |
| `ScramLoginModule` | SCRAM 认证 | `user="admin" password="hash"` |
| `Krb5LoginModule` | Kerberos 认证 | `useKeyTab=true storeKey=true` |
| `DigestLoginModule` | DIGEST-MD5 认证 | （已废弃） |

---

### **4. Configuration（配置）**

定义使用哪些 LoginModule 及其配置参数。

**配置格式：**
```java
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
        username="admin"
        password="admin-secret";
};
```


---

## 🔧 **JAAS 配置文件示例**

### **文件形式：`jaas.conf`**

```bash
# Kafka Server 的 JAAS 配置
KafkaServer {
    // PLAIN 认证机制
    org.apache.kafka.common.security.plain.PlainLoginModule required
        username="admin"
        password="admin-secret"
        user_admin="admin-secret"
        user_alice="alice-secret";
        
    // SCRAM-SHA-256 认证机制
    org.apache.kafka.common.security.scram.ScramLoginModule required
        username="broker-admin"
        password="broker-secret";
};

// Kafka Client 的 JAAS 配置
KafkaClient {
    org.apache.kafka.common.security.scram.ScramLoginModule required
        username="client-user"
        password="client-password";
};
```


**启动时指定：**
```bash
export KAFKA_OPTS="-Djava.security.auth.login.config=/path/to/jaas.conf"
kafka-server-start.sh config/server.properties
```


---

## 💡 **Kafka 中的 JAAS 应用**

### **场景 1：Server 端配置（动态配置 - 推荐）**

```properties
# server.properties

# Listener 定义
listeners=SASL_SSL://0.0.0.0:9093

# SASL 机制
sasl.enabled.mechanisms=SCRAM-SHA-256

# Listener 级别的 JAAS 配置（Kafka 内部处理）
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="broker-admin" \
    password="broker-secret";
```


**对应代码（JaasContext.java:128）：**
```java
Password dynamicJaasConfig = configs.get(
    "scram-sha-256.sasl.jaas.config"  // ← mechanism 前缀
);
```


---

### **场景 2：Server 端配置（静态文件）**

```bash
# jaas-server.conf
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
        username="broker-admin"
        password="broker-secret";
        
    org.apache.kafka.common.security.plain.PlainLoginModule required
        username="admin"
        password="admin-secret"
        user_admin="admin-secret"
        user_alice="alice-secret";
};
```


**启动命令：**
```bash
export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/jaas-server.conf"
kafka-server-start.sh config/server.properties
```


---

### **场景 3：Client 端配置（Producer/Consumer）**

```properties
# client.properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256

# 方式 1：直接在配置文件中指定（推荐）
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="producer-user" \
    password="producer-password";

# 方式 2：使用 JAAS 配置文件
# export KAFKA_OPTS="-Djava.security.auth.login.config=/path/to/jaas-client.conf"
```


**Java 代码示例：**
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9093");
props.put("security.protocol", "SASL_SSL");
props.put("sasl.mechanism", "SCRAM-SHA-256");
props.put("sasl.jaas.config", 
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"producer-user\" " +
    "password=\"producer-password\";");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```


---

## 🔍 **JaasContext 类的作用**

### **核心功能：**
加载和管理 JAAS 配置上下文，为 Kafka 的 SASL 认证提供配置信息。

---

### **两个主要方法：**

#### **1. `loadServerContext()` - 服务端上下文**

```java
// JaasContext.java:122
public static JaasContext loadServerContext(
    ListenerName listenerName,   // 监听器名称
    String mechanism,            // SASL 机制
    Map<String, ?> configs       // 配置映射
) {
    // 查找配置：{mechanism}.sasl.jaas.config
    Password dynamicJaasConfig = configs.get(
        mechanism.toLowerCase() + ".sasl.jaas.config"
    );
    
    // 构建上下文名称：{listenerName}.KafkaServer
    String listenerContextName = listenerName.value().toLowerCase() + ".KafkaServer";
    
    // 加载配置
    return load(Type.SERVER, listenerContextName, "KafkaServer", dynamicJaasConfig);
}
```


**调用示例：**
```java
// ChannelBuilders.java:144
JaasContext context = JaasContext.loadServerContext(
    new ListenerName("EXTERNAL"),
    "SCRAM-SHA-256",
    configs
);

// 返回的 JaasContext 包含：
context.name() = "KafkaServer"
context.type() = Type.SERVER
context.configuration() // JAAS Configuration 对象
```


---

#### **2. `loadClientContext()` - 客户端上下文**

```java
// JaasContext.java:142
public static JaasContext loadClientContext(Map<String, ?> configs) {
    // 查找配置：sasl.jaas.config（不需要前缀）
    Password dynamicJaasConfig = configs.get("sasl.jaas.config");
    
    // 客户端上下文名称固定为 "KafkaClient"
    return load(Type.CLIENT, null, "KafkaClient", dynamicJaasConfig);
}
```


---

## 🏗️ **JAAS 在 Kafka 中的工作流程**

### **完整认证流程：**

```
┌─────────────────────────────────────────┐
│ 1. Kafka Server 启动                     │
├─────────────────────────────────────────┤
│ • 读取配置                               │
│ • 调用 JaasContext.loadServerContext()   │
│ • 创建 JaasContext 对象                  │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ 2. Client 发起连接请求                    │
├─────────────────────────────────────────┤
│ • Socket 连接建立                        │
│ • SASL 握手开始                          │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ 3. Server 端进行认证                      │
├─────────────────────────────────────────┤
│ • 从 JaasContext 获取 LoginModule        │
│ • 调用 LoginModule.login()               │
│ • 验证用户名密码                         │
│ • 创建 Subject 和 Principal              │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ 4. 认证成功/失败                          │
├─────────────────────────────────────────┤
│ ✅ 成功：建立连接，允许通信              │
│ ❌ 失败：关闭连接，返回错误              │
└─────────────────────────────────────────┘
```


---

## 📊 **JAAS vs SASL 的关系**

很多人容易混淆这两个概念：

| 特性 | JAAS | SASL |
|------|------|------|
| **全称** | Java Authentication and Authorization Service | Simple Authentication and Security Layer |
| **定位** | Java 的安全框架 API | IETF 标准的认证协议 |
| **作用** | 提供认证和授权的编程接口 | 定义认证过程的协议规范 |
| **关系** | JAAS 使用 SASL 进行实际认证 | SASL 通过 JAAS 在 Java 中实现 |

**类比：**
```
JAAS = 汽车的驾驶舱（方向盘、踏板、仪表盘）
SASL = 发动机（实际提供动力）

驾驶员（Kafka）通过驾驶舱（JAAS）控制发动机（SASL）来驱动汽车
```


---

## ⚠️ **安全注意事项**

### **1. 禁止使用的 LoginModule**

```java
// JaasContext.java:163-172
private static void throwIfLoginModuleIsNotAllowed(AppConfigurationEntry entry) {
    // 系统属性：zookeeper.sasl.blacklist.modules
    Set<String> disallowedModules = Arrays.stream(
        System.getProperty(DISALLOWED_LOGIN_MODULES_CONFIG, DISALLOWED_LOGIN_MODULES_DEFAULT)
            .split(",")
    ).collect(Collectors.toSet());
    
    if (disallowedModules.contains(entry.getLoginModuleName())) {
        throw new IllegalArgumentException(
            entry.getLoginModuleName() + " is not allowed"
        );
    }
}
```


**默认黑名单：**
```properties
# 不允许使用 com.sun.security.auth.module.UnixLoginModule
# 防止本地用户提权
zookeeper.sasl.blacklist.modules=com.sun.security.auth.module.UnixLoginModule
```


---

### **2. 密码保护**

```java
// 配置中的密码类型是 Password（加密包装）
Password dynamicJaasConfig = (Password) configs.get(...);

// Password 类会对敏感信息进行掩码处理
String maskedValue = dynamicJaasConfig.value(); // 实际值
String displayValue = dynamicJaasConfig.toString(); // "***"
```


---

## 🎯 **总结**

### **JAAS 的核心价值：**

1. ✅ **统一接口**：标准化的认证授权 API
2. ✅ **可插拔**：支持多种认证机制（PLAIN、SCRAM、Kerberos 等）
3. ✅ **灵活性**：支持多 Listener、多机制配置
4. ✅ **安全性**：密码保护、模块黑名单等安全机制

### **在 Kafka 中的应用：**

```
Kafka Security = SASL（认证协议） + JAAS（配置管理） + SSL/TLS（加密传输）
```


**JaasContext 的作用：**
- 🎯 加载 JAAS 配置（动态或静态）
- 🎯 创建认证上下文
- 🎯 为 SASL 认证提供 LoginModule 配置
- 🎯 支持服务端和客户端两种模式

这是 Kafka 企业级安全体系的基石！🔒
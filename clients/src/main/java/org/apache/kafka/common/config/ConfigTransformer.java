/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.provider.FileConfigProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class wraps a set of {@link ConfigProvider} instances and uses them to perform
 * transformations.
 *
 * <p>The default variable pattern is of the form <code>${provider:[path:]key}</code>,
 * where the <code>provider</code> corresponds to a {@link ConfigProvider} instance, as passed to
 * {@link ConfigTransformer#ConfigTransformer(Map)}.  The pattern will extract a set
 * of paths (which are optional) and keys and then pass them to {@link ConfigProvider#get(String, Set)} to obtain the
 * values with which to replace the variables.
 *
 * <p>For example, if a Map consisting of an entry with a provider name "file" and provider instance
 * {@link FileConfigProvider} is passed to the {@link ConfigTransformer#ConfigTransformer(Map)}, and a Properties
 * file with contents
 * <pre>
 * fileKey=someValue
 * </pre>
 * resides at the path "/tmp/properties.txt", then when a configuration Map which has an entry with a key "someKey" and
 * a value "${file:/tmp/properties.txt:fileKey}" is passed to the {@link #transform(Map)} method, then the transformed
 * Map will have an entry with key "someKey" and a value "someValue".
 *
 * <p>This class only depends on {@link ConfigProvider#get(String, Set)} and does not depend on subscription support
 * in a {@link ConfigProvider}, such as the {@link ConfigProvider#subscribe(String, Set, ConfigChangeCallback)} and
 * {@link ConfigProvider#unsubscribe(String, Set, ConfigChangeCallback)} methods.
 */
public class ConfigTransformer {
    public static final Pattern DEFAULT_PATTERN = Pattern.compile("\\$\\{([^}]*?):(([^}]*?):)?([^}]*?)\\}");
    private static final String EMPTY_PATH = "";

    private final Map<String, ConfigProvider> configProviders;

    /**
     * Creates a ConfigTransformer with the default pattern, of the form <code>${provider:[path:]key}</code>.
     *
     * @param configProviders a Map of provider names and {@link ConfigProvider} instances.
     */
    public ConfigTransformer(Map<String, ConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Transforms the given configuration data by using the {@link ConfigProvider} instances to
     * look up values to replace the variables in the pattern.
     *
     * @param configs the configuration values to be transformed
     * @return an instance of {@link ConfigTransformerResult}
     *
     * 扫描所有配置项，找出其中的变量引用（如 ${file:/path:key}），然后调用对应的 ConfigProvider 获取实际值并替换。
     * 输入：Map<String, String> - 所有字符串类型的配置
     * 处理：扫描每个配置的值（value）
     * 目标：找出形如 ${provider:path:key} 的变量
     *
     * # ========== 基础配置（无变量）==========
     * bootstrap.servers=localhost:9092
     * client.id=my-producer
     * acks=all
     *
     * # ========== 包含变量的配置 ==========
     * # 1. SSL 配置 - 从文件读取密码
     * ssl.keystore.password=${file:/etc/kafka/secrets/keystore.properties:password}
     * ssl.truststore.password=${file:/etc/kafka/secrets/truststore.properties:password}
     * ssl.key.password=${file:/etc/kafka/secrets/ssl-keys.properties:key_password}
     *
     * # 2. SASL 配置 - 从 Vault 读取凭证
     * sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
     *     username="producer-user" \
     *     password="${vault:/secret/kafka/producer:sasl_password}";
     *
     * # 3. 从环境变量读取
     * metric.reporters=${env:KAFKA_METRIC_REPORTERS}
     * client.dns.lookup=${env:CLIENT_DNS_LOOKUP:use_all_dns_ips}
     *
     * # 4. 混合多个 provider
     * ssl.endpoint.identification.algorithm=${env:SSL_ENDPOINT_ALGO:HTTPS}
     *
     * ${<provider-name>:[<path>:]<key>}
     * ```
     * | 部分 | 说明 | 是否必需 | 示例 |
     * |------|------|----------|------|
     * | `provider-name` | ConfigProvider 的名称 | ✅ 必需 | `file`, `vault`, `env` |
     * | `path` | 资源路径（由 provider 定义） | ❌ 可选 | `/etc/kafka/secrets` |
     * | `key` | 配置键名 | ✅ 必需 | `password`, `username` |
     *
     * 1️⃣ FileConfigProvider（从文件读取）
     * # 配置 Provider
     * config.providers=file
     * config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
     * config.providers.file.param.allowed.paths=/etc/kafka/secrets
     *
     * # 文件格式：Java Properties
     * # /etc/kafka/secrets/keystore.properties
     * password=keystore_secret_123
     * type=PKCS12
     *
     * # /etc/kafka/secrets/truststore.properties
     * password=truststore_secret_456
     *
     * # 使用变量
     * ssl.keystore.password=${file:/etc/kafka/secrets/keystore.properties:password}
     * ssl.keystore.type=${file:/etc/kafka/secrets/keystore.properties:type}
     *
     * 2️⃣ EnvVarConfigProvider（从环境变量读取）
     * # 配置 Provider
     * config.providers=env
     * config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
     *
     * # 使用变量（无 path，直接读环境变量）
     * metric.reporters=${env:KAFKA_METRIC_REPORTERS}
     * client.dns.lookup=${env:CLIENT_DNS_LOOKUP}
     *
     * # 带默认值（如果环境变量不存在，使用默认值）
     * compression.type=${env:COMPRESSION_TYPE:lz4}
     *
     * 3️⃣ VaultConfigProvider（从 HashiCorp Vault 读取）
     * # 配置 Provider
     * config.providers=vault
     * config.providers.vault.class=com.example.VaultConfigProvider
     * config.providers.vault.param.vault.url=https://vault.example.com:8200
     * config.providers.vault.param.vault.token=hvs.xxxxx
     *
     * # Vault 中的数据结构
     * # secret/kafka/producer
     * #   sasl_username = producer-user
     * #   sasl_password = vault_producer_pwd_123
     *
     * # secret/kafka/broker
     * #   admin_password = vault_broker_pwd_456
     *
     * # 使用变量
     * sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
     *     username="${vault:/secret/kafka/producer:sasl_username}" \
     *     password="${vault:/secret/kafka/producer:sasl_password}";
     *
     * 4️⃣ 混合使用多个 Provider
     * # 声明多个 provider
     * config.providers=file,vault,env
     *
     * # file provider
     * config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
     * config.providers.file.param.allowed.paths=/etc/kafka/configs
     *
     * # vault provider
     * config.providers.vault.class=com.example.VaultConfigProvider
     * config.providers.vault.param.vault.url=https://vault.internal:8200
     *
     * # env provider
     * config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
     *
     * # ========== 实际配置使用 ==========
     * # 基础连接
     * bootstrap.servers=localhost:9092
     *
     * # SSL 配置从文件读取
     * ssl.truststore.location=/etc/kafka/configs/kafka.truststore.jks
     * ssl.truststore.password=${file:/etc/kafka/configs/ssl.properties:truststore_password}
     * ssl.keystore.location=/etc/kafka/configs/kafka.keystore.jks
     * ssl.keystore.password=${file:/etc/kafka/configs/ssl.properties:keystore_password}
     *
     * # SASL 配置从 Vault 读取
     * listener.name.sasl_ssl/plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
     *     username="broker" \
     *     password="${vault:/secret/kafka/broker:admin_password}";
     *
     * # 监控配置从环境变量读取
     * metric.reporters=${env:KAFKA_METRIC_REPORTERS:io.confluent.metrics.reporter.ConfluentMetricsReporter}
     * confluent.metrics.reporter.bootstrap.servers=${env:METRIC_BOOTSTRAP_SERVERS:localhost:9092}
     *
     * 🔍 特殊场景处理
     * 场景 1：一个配置项包含多个变量
     * # 配置
     * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
     *     username="${vault:/secret/kafka:user}" \
     *     password="${vault:/secret/kafka:pass}";
     *
     * # 处理过程
     * // 提取到两个变量：
     * // 1. ${vault:/secret/kafka:user}
     * // 2. ${vault:/secret/kafka:pass}
     *
     * // 调用一次 provider.get("/secret/kafka", ["user", "pass"])
     * // 返回：
     * // {
     * //   "user": "admin",
     * //   "pass": "secret123"
     * // }
     *
     * // 替换后：
     * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
     *     username="admin" \
     *     password="secret123";
     *
     * 场景 2：变量在值中间
     * # 配置
     * ssl.protocol=${env:SSL_PROTOCOL:TLSv1.2}
     *
     * # 替换后
     * ssl.protocol=TLSv1.3  # 如果 SSL_PROTOCOL=TLSv1.3
     * ssl.protocol=TLSv1.2  # 如果 SSL_PROTOCOL 未设置（使用默认值）
     *
     * 场景 3：无法解析的变量保留原样
     * // 如果 provider 找不到或 key 不存在
     * ConfigData configData = provider.get(path, keys);
     * // 返回空数据
     *
     * // 变量不会被替换，保持原样
     * "${vault:/nonexistent:key}" → "${vault:/nonexistent:key}"
     *
     * 📋 完整配置模板
     * # ============================================
     * # Kafka ConfigProvider 完整配置示例
     * # ============================================
     *
     * # ----- 1. 声明使用的 Provider -----
     * config.providers=file,vault,env
     *
     * # ----- 2. 配置 FileConfigProvider -----
     * config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
     * config.providers.file.param.allowed.paths=/etc/kafka/secrets,/var/lib/kafka/config
     *
     * # ----- 3. 配置 VaultConfigProvider -----
     * config.providers.vault.class=com.example.security.VaultConfigProvider
     * config.providers.vault.param.vault.url=https://vault.example.com:8200
     * config.providers.vault.param.vault.token.path=/var/run/vault/token
     * config.providers.vault.param.vault.path=secret/kafka
     *
     * # ----- 4. 配置 EnvVarConfigProvider -----
     * config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
     *
     * # ============================================
     * # 实际业务配置
     * # ============================================
     *
     * # Broker 基础配置
     * broker.id=0
     * listeners=SASL_SSL://0.0.0.0:9093
     * advertised.listeners=SASL_SSL://broker1.example.com:9093
     *
     * # SSL 配置 - 从文件读取
     * ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
     * ssl.truststore.password=${file:/etc/kafka/secrets/ssl.properties:truststore_password}
     * ssl.keystore.location=/etc/kafka/secrets/kafka.keystore.jks
     * ssl.keystore.password=${file:/etc/kafka/secrets/ssl.properties:keystore_password}
     * ssl.key.password=${file:/etc/kafka/secrets/ssl.properties:key_password}
     *
     * # SASL 配置 - 从 Vault 读取
     * listener.name.sasl_ssl/plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
     *     username="broker" \
     *     password="${vault:/secret/kafka/broker:admin_password}";
     *
     * # 指标报告 - 从环境变量读取
     * metric.reporters=${env:KAFKA_METRIC_REPORTERS}
     * confluent.metrics.reporter.bootstrap.servers=${env:METRIC_BOOTSTRAP_SERVERS:localhost:9092}
     *
     * # ZooKeeper 配置 - 混合使用
     * zookeeper.connect=${env:ZOOKEEPER_CONNECT:localhost:2181}
     * zookeeper.connection.timeout.ms=18000
     *
     * ConfigTransformer.transform() 方法处理的配置：
     * ┌─────────────────────────────────────────────┐
     * │ 1. 输入：所有字符串类型的配置项              │
     * │    Map<String, String>                      │
     * ├─────────────────────────────────────────────┤
     * │ 2. 识别：包含 ${provider:path:key} 的配置    │
     * │    通过正则表达式提取变量                   │
     * ├─────────────────────────────────────────────┤
     * │ 3. 分组：按 provider 和 path 组织请求        │
     * │    优化查询，批量获取数据                   │
     * ├─────────────────────────────────────────────┤
     * │ 4. 查询：调用 ConfigProvider.get(path, keys) │
     * │    从外部源获取实际值                        │
     * ├─────────────────────────────────────────────┤
     * │ 5. 替换：将变量替换为实际值                  │
     * │    无法解析的变量保持原样                   │
     * ├─────────────────────────────────────────────┤
     * │ 6. 输出：转换后的配置 + TTL 信息             │
     * │    ConfigTransformerResult                  │
     * └─────────────────────────────────────────────┘
     */
    public ConfigTransformerResult transform(Map<String, String> configs) {
        Map<String, Map<String, Set<String>>> keysByProvider = new HashMap<>();
        Map<String, Map<String, Map<String, String>>> lookupsByProvider = new HashMap<>();

        // Collect the variables from the given configs that need transformation 遍历所有配置项
        /**
         * ssl.keystore.password=${file:/etc/kafka/secrets/keystore.properties:password}
         * ssl.truststore.password=${file:/etc/kafka/secrets/truststore.properties:password}
         * sasl.password=${vault:/secret/kafka:sasl_password}
         * metric.reporters=${env:KAFKA_METRIC_REPORTERS}
         * 处理完：
         * keysByProvider = {
         *     "file" -> {
         *         "/etc/kafka/secrets/keystore.properties" -> ["password"],
         *         "/etc/kafka/secrets/truststore.properties" -> ["password"]
         *     },
         *     "vault" -> {
         *         "/secret/kafka" -> ["sasl_password"]
         *     },
         *     "env" -> {
         *         "" -> ["KAFKA_METRIC_REPORTERS"]
         *     }
         * }
         *
         */
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (config.getValue() != null) {
                // 提取值中的所有变量
                List<ConfigVariable> configVars = getVars(config.getValue(), DEFAULT_PATTERN);
                for (ConfigVariable configVar : configVars) {
                    // 按 provider 名称分组
                    Map<String, Set<String>> keysByPath = keysByProvider.computeIfAbsent(configVar.providerName, k -> new HashMap<>());
                    // 按 path 分组
                    Set<String> keys = keysByPath.computeIfAbsent(configVar.path, k -> new HashSet<>());
                    // 记录变量名
                    keys.add(configVar.variable);
                }
            }
        }

        // Retrieve requested variables from the ConfigProviders 遍历每个 provider
        /**
         * lookupsByProvider = {
         *     "file" -> {
         *         "/etc/kafka/secrets/keystore.properties" -> {
         *             "password": "keystore_secret_123"
         *         },
         *         "/etc/kafka/secrets/truststore.properties" -> {
         *             "password": "truststore_secret_456"
         *         }
         *     },
         *     "vault" -> {
         *         "/secret/kafka" -> {
         *             "sasl_password": "vault_sasl_pwd_789"
         *         }
         *     },
         *     "env" -> {
         *         "" -> {
         *             "KAFKA_METRIC_REPORTERS": "io.confluent.metrics.reporter.ConfluentMetricsReporter"
         *         }
         *     }
         * }
         *
         * ttls = {
         *     "/secret/kafka": 300000  // 5 分钟 TTL
         * }
          */
        Map<String, Long> ttls = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> entry : keysByProvider.entrySet()) {
            String providerName = entry.getKey();
            ConfigProvider provider = configProviders.get(providerName);
            Map<String, Set<String>> keysByPath = entry.getValue();
            if (provider != null && keysByPath != null) {
                // 对每个 path 调用 provider.get(path, keys)
                for (Map.Entry<String, Set<String>> pathWithKeys : keysByPath.entrySet()) {
                    String path = pathWithKeys.getKey();
                    Set<String> keys = new HashSet<>(pathWithKeys.getValue());
                    // ⭐ 关键：调用 ConfigProvider 获取配置
                    ConfigData configData = provider.get(path, keys);
                    Map<String, String> data = configData.data();
                    Long ttl = configData.ttl();
                    // 保存 TTL（用于配置刷新）
                    if (ttl != null && ttl >= 0) {
                        ttls.put(path, ttl);
                    }
                    // 保存获取到的键值对
                    Map<String, Map<String, String>> keyValuesByPath =
                            lookupsByProvider.computeIfAbsent(providerName, k -> new HashMap<>());
                    keyValuesByPath.put(path, data);
                }
            }
        }

        // Perform the transformations by performing variable replacements  复制原始配置
        /**
         * // 原始配置
         * {
         *     "ssl.keystore.password": "${file:/etc/kafka/secrets/keystore.properties:password}",
         *     "sasl.password": "${vault:/secret/kafka:sasl_password}"
         * }
         *
         * // 替换后
         * {
         *     "ssl.keystore.password": "keystore_secret_123",
         *     "sasl.password": "vault_sasl_pwd_789"
         * }
          */
        Map<String, String> data = new HashMap<>(configs);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            // 逐个替换变量
            data.put(config.getKey(), replace(lookupsByProvider, config.getValue(), DEFAULT_PATTERN));
        }
        return new ConfigTransformerResult(data, ttls);
    }

    private static List<ConfigVariable> getVars(String value, Pattern pattern) {
        List<ConfigVariable> configVars = new ArrayList<>();
        Matcher matcher = pattern.matcher(value);
        while (matcher.find()) {
            configVars.add(new ConfigVariable(matcher));
        }
        return configVars;
    }

    private static String replace(Map<String, Map<String, Map<String, String>>> lookupsByProvider,
                                  String value,
                                  Pattern pattern) {
        if (value == null) {
            return null;
        }
        Matcher matcher = pattern.matcher(value);
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            ConfigVariable configVar = new ConfigVariable(matcher);
            Map<String, Map<String, String>> lookupsByPath = lookupsByProvider.get(configVar.providerName);
            if (lookupsByPath != null) {
                Map<String, String> keyValues = lookupsByPath.get(configVar.path);
                String replacement = keyValues.get(configVar.variable);
                builder.append(value, i, matcher.start());
                if (replacement == null) {
                    // No replacements will be performed; just return the original value
                    builder.append(matcher.group(0));
                } else {
                    builder.append(replacement);
                }
                i = matcher.end();
            }
        }
        builder.append(value, i, value.length());
        return builder.toString();
    }

    private static class ConfigVariable {
        final String providerName;
        final String path;
        final String variable;

        /**
         * \$\{([^}]*?):(([^}]*?):)?([^}]*?)\}
         * 分解为可读格式
         *  \$           # 匹配字面量 $
         * \{           # 匹配字面量 {
         * ([^}]*?)     # ← Group 1: provider 名称（非贪婪匹配）
         * :            # 匹配冒号 :
         * (            # ← Group 2: 整个 path: 部分（包括末尾的冒号）
         *   ([^}]*?)   # ← Group 3: path 路径（可选）
         *   :          # 匹配冒号 :
         * )?           # Group 2 整体是可选的（?）
         * ([^}]*?)     # ← Group 4: key 键名
         * \}           # 匹配字面量 }
         *
         * String variable = "${file:/etc/kafka/secrets/keystore.properties:password}";
         * Matcher matcher = DEFAULT_PATTERN.matcher(variable);
         * matcher.matches();
         *
         * // 分组对应关系：
         * matcher.group(0) → "${file:/etc/kafka/secrets/keystore.properties:password}"  // 整个匹配
         * matcher.group(1) → "file"                          // provider 名称 ⭐
         * matcher.group(2) → "/etc/kafka/secrets/keystore.properties:"  // path + 冒号（整个可选部分）
         * matcher.group(3) → "/etc/kafka/secrets/keystore.properties"  // path 路径 ⭐
         * matcher.group(4) → "password"                      // key 键名 ⭐
         *
         * ${file:/etc/kafka/secrets/keystore.properties:password}
         * │                                │
         * └─────── 整个匹配 (group 0) ──────┘
         *
         *   ${ file : /etc/kafka/secrets/keystore.properties : password }
         *      └─┬─┘                        └──┬──┘        └─┬─┘
         *        │                            │              │
         *        │                            │              └── group(4): "password"
         *        │                            │
         *        │                            └── group(3): "/etc/kafka/secrets/keystore.properties"
         *        │
         *        └── group(1): "file"
         *
         *   ${ file : [ /etc/kafka/secrets/keystore.properties : ] password }
         *             └────────────┬─────────────┘
         *                          │
         *                          └── group(2): 整个可选部分（包含 path 和冒号）
         * 正则表达式分组的编号规则：
         * ┌─────────────────────────────────────────────┐
         * │ • 按左括号 "(" 出现的顺序从左到右编号        │
         * │ • group(0) = 整个匹配的字符串               │
         * │ • 每个 "(" 开始一个新的分组                 │
         * ├─────────────────────────────────────────────┤
         * │ 对于模式：\$\{([^}]*?):(([^}]*?):)?([^}]*?)\}│
         * │                                             │
         * │   Group 1: ([^}]*?)     → provider 名称     │
         * │   Group 2: (([^}]*?):)? → 整个可选部分      │
         * │   Group 3: ([^}]*?)     → path 路径         │
         * │   Group 4: ([^}]*?)     → key 键名          │
         * ├─────────────────────────────────────────────┤
         * │ 所以代码中使用：                            │
         * │   matcher.group(1) → providerName           │
         * │   matcher.group(3) → path (跳过 group 2)    │
         * │   matcher.group(4) → variable               │
         * └─────────────────────────────────────────────┘
         */
        ConfigVariable(Matcher matcher) {
            this.providerName = matcher.group(1);
            this.path = matcher.group(3) != null ? matcher.group(3) : EMPTY_PATH;
            this.variable = matcher.group(4);
        }

        public String toString() {
            return "(" + providerName + ":" + (path != null ? path + ":" : "") + variable + ")";
        }
    }
}

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
package org.apache.kafka.common.network;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class ChannelBuilders {
    private static final Logger log = LoggerFactory.getLogger(ChannelBuilders.class);

    private ChannelBuilders() { }

    /**
     * @param securityProtocol the securityProtocol
     * @param contextType the contextType, it must be non-null if `securityProtocol` is SASL_*; it is ignored otherwise
     * @param config client config
     * @param listenerName the listenerName if contextType is SERVER or null otherwise
     * @param clientSaslMechanism SASL mechanism if mode is CLIENT, ignored otherwise
     * @param time the time instance
     * @param saslHandshakeRequestEnable flag to enable Sasl handshake requests; disabled only for SASL
     *             inter-broker connections with inter-broker protocol version < 0.10
     * @param logContext the log context instance
     *
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    public static ChannelBuilder clientChannelBuilder(
            SecurityProtocol securityProtocol,
            JaasContext.Type contextType,
            AbstractConfig config,
            ListenerName listenerName,
            String clientSaslMechanism,
            Time time,
            boolean saslHandshakeRequestEnable,
            LogContext logContext) {

        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (contextType == null)
                throw new IllegalArgumentException("`contextType` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException("`clientSaslMechanism` must be non-null in client mode if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, ConnectionMode.CLIENT, contextType, config, listenerName, false, clientSaslMechanism,
                saslHandshakeRequestEnable, null, null, time, logContext, null);
    }

    /**
     * @param listenerName the listenerName
     * @param isInterBrokerListener whether or not this listener is used for inter-broker requests
     * @param securityProtocol the securityProtocol
     * @param config server config
     * @param credentialCache Credential cache for SASL/SCRAM if SCRAM is enabled
     * @param tokenCache Delegation token cache
     * @param time the time instance
     * @param logContext the log context instance
     * @param apiVersionSupplier supplier for ApiVersions responses sent prior to authentication
     *
     * @return the configured `ChannelBuilder`
     */
    public static ChannelBuilder serverChannelBuilder(ListenerName listenerName,
                                                      boolean isInterBrokerListener,
                                                      SecurityProtocol securityProtocol,
                                                      AbstractConfig config,
                                                      CredentialCache credentialCache,
                                                      DelegationTokenCache tokenCache,
                                                      Time time,
                                                      LogContext logContext,
                                                      Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        return create(securityProtocol, ConnectionMode.SERVER, JaasContext.Type.SERVER, config, listenerName,
                isInterBrokerListener, null, true, credentialCache,
                tokenCache, time, logContext, apiVersionSupplier);
    }

    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         ConnectionMode connectionMode,
                                         JaasContext.Type contextType,
                                         AbstractConfig config,
                                         ListenerName listenerName,
                                         boolean isInterBrokerListener,
                                         String clientSaslMechanism,
                                         boolean saslHandshakeRequestEnable,
                                         CredentialCache credentialCache,
                                         DelegationTokenCache tokenCache,
                                         Time time,
                                         LogContext logContext,
                                         Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        Map<String, Object> configs = channelBuilderConfigs(config, listenerName);

        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SSL:
                requireNonNullMode(connectionMode, securityProtocol);
                channelBuilder = new SslChannelBuilder(connectionMode, listenerName, isInterBrokerListener, logContext);
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                requireNonNullMode(connectionMode, securityProtocol);
                Map<String, JaasContext> jaasContexts;
                String sslClientAuthOverride = null;
                if (connectionMode == ConnectionMode.SERVER) {
                    @SuppressWarnings("unchecked")
                    List<String> enabledMechanisms = (List<String>) configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);// sasl.enabled.mechanisms
                    jaasContexts = new HashMap<>(enabledMechanisms.size());
                    for (String mechanism : enabledMechanisms)
                        jaasContexts.put(mechanism, JaasContext.loadServerContext(listenerName, mechanism, configs));

                    // SSL client authentication is enabled in brokers for SASL_SSL only if listener-prefixed config is specified.
                    if (listenerName != null && securityProtocol == SecurityProtocol.SASL_SSL) {
                        String configuredClientAuth = (String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);// "ssl.client.auth"
                        String listenerClientAuth = (String) config.originalsWithPrefix(listenerName.configPrefix(), true)
                                .get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);

                        // If `ssl.client.auth` is configured at the listener-level, we don't set an override and SslFactory
                        // uses the value from `configs`. If not, we propagate `sslClientAuthOverride=NONE` to SslFactory and
                        // it applies the override to the latest configs when it is configured or reconfigured. `Note that
                        // ssl.client.auth` cannot be dynamically altered.
                        if (listenerClientAuth == null) {
                            sslClientAuthOverride = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
                            if (configuredClientAuth != null && !configuredClientAuth.equalsIgnoreCase(SslClientAuth.NONE.name())) {
                                log.warn("Broker configuration '{}' is applied only to SSL listeners. Listener-prefixed configuration can be used" +
                                        " to enable SSL client authentication for SASL_SSL listeners. In future releases, broker-wide option without" +
                                        " listener prefix may be applied to SASL_SSL listeners as well. All configuration options intended for specific" +
                                        " listeners should be listener-prefixed.", BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG);
                            }
                        }
                    }
                } else {
                    // Use server context for inter-broker client connections and client context for other clients
                    JaasContext jaasContext = contextType == JaasContext.Type.CLIENT ? JaasContext.loadClientContext(configs) :
                            JaasContext.loadServerContext(listenerName, clientSaslMechanism, configs);
                    jaasContexts = Collections.singletonMap(clientSaslMechanism, jaasContext);
                }
                channelBuilder = new SaslChannelBuilder(connectionMode,
                        jaasContexts,
                        securityProtocol,
                        listenerName,
                        isInterBrokerListener,
                        clientSaslMechanism,
                        saslHandshakeRequestEnable,
                        credentialCache,
                        tokenCache,
                        sslClientAuthOverride,
                        time,
                        logContext,
                        apiVersionSupplier);
                break;
            case PLAINTEXT:
                channelBuilder = new PlaintextChannelBuilder(listenerName);
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

    /**
     * 确保 Kafka 的 Listener 级别配置 > SASL 机制级别配置 > 全局通用配置 的优先级顺序，避免配置冲突和重复。
     * 📋 Kafka 配置的三层结构
     *  配置优先级（从高到低）：
     *   1️⃣ Listener 特定配置：listener.name.{listener}.{config}
     *   2️⃣ SASL 机制特定配置：{mechanism}.{config}
     *   3️⃣ 全局通用配置：{config}
     *
     * # ==================== 全局通用配置 ====================
     * security.protocol=SASL_SSL
     * sasl.mechanism=SCRAM-SHA-256
     * ssl.keystore.location=/default/keystore.jks
     * ssl.truststore.location=/default/truststore.jks
     * scram-sha-256.password=default_password
     *
     * # ==================== INTERNAL Listener 配置 ====================
     * listener.name.internal.security.protocol=SASL_PLAINTEXT
     * listener.name.internal.sasl.mechanism=PLAIN
     * listener.name.internal.plain.username=internal_user
     * listener.name.internal.plain.password=internal_password
     *
     * # ==================== EXTERNAL Listener 配置 ====================
     * listener.name.external.security.protocol=SASL_SSL
     * listener.name.external.ssl.keystore.location=/external/keystore.jks
     * listener.name.external.ssl.truststore.location=/external/truststore.jks
     * listener.name.external.scram-sha-256.password=external_password
     *
     * 步骤 1：调用 valuesWithPrefixOverride("listener.name.external.")
     * // AbstractConfig.java:490-507
     * Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
     * // values() 返回所有不带前缀的全局配置：
     * result = {
     *     "security.protocol": "SASL_SSL",
     *     "sasl.mechanism": "SCRAM-SHA-256",
     *     "ssl.keystore.location": "/default/keystore.jks",
     *     "ssl.truststore.location": "/default/truststore.jks",
     *     "scram-sha-256.password": "default_password"
     * }
     *
     * // 遍历 originals，查找带前缀的配置
     * for (Map.Entry<String, ?> entry : originals.entrySet()) {
     *     if (entry.getKey().startsWith("listener.name.external.")) {
     *         String keyWithNoPrefix = entry.getKey().substring(25); // 去掉前缀
     *
     *         // 情况 A：直接匹配
     *         // "listener.name.external.ssl.keystore.location"
     *         // → keyWithNoPrefix = "ssl.keystore.location"
     *         // → definition.configKeys().get("ssl.keystore.location") ✅ 存在
     *         // → result.put("ssl.keystore.location", "/external/keystore.jks")
     *         // 覆盖全局配置！
     *
     *         // 情况 B：需要去掉二级前缀
     *         // "listener.name.external.scram-sha-256.password"
     *         // → keyWithNoPrefix = "scram-sha-256.password"
     *         // → definition.configKeys().get("scram-sha-256.password") ❌ 不存在
     *         // → keyWithNoSecondaryPrefix = "password" (去掉 "scram-sha-256.")
     *         // → definition.configKeys().get("password") ❌ 也不存在
     *         // → 不添加
     *     }
     * }
     *
     * // 最终 parsedConfigs：
     * parsedConfigs = {
     *     "security.protocol": "SASL_SSL",
     *     "sasl.mechanism": "SCRAM-SHA-256",
     *     "ssl.keystore.location": "/external/keystore.jks",      // ← 被 Listener 配置覆盖
     *     "ssl.truststore.location": "/external/truststore.jks",  // ← 被 Listener 配置覆盖
     *     "scram-sha-256.password": "default_password"            // ← 保持全局配置
     * }
     * 步骤 2：应用三个 filter 补充配置
     * config.originals().entrySet().stream()
     *     // 原始配置清单：
     *     .filter(e -> !parsedConfigs.containsKey(e.getKey()))
     *     // 排除已存在的 key：
     *     // ❌ "security.protocol" - 已存在
     *     // ❌ "sasl.mechanism" - 已存在
     *     // ❌ "ssl.keystore.location" - 已存在
     *     // ❌ "ssl.truststore.location" - 已存在
     *     // ❌ "scram-sha-256.password" - 已存在
     *     // ❌ "listener.name.internal.*" - 不以 external 开头，保留
     *     // ❌ "listener.name.external.*" - 会被场景 2 排除
     *
     *     .filter(e -> !(listenerName != null && e.getKey().startsWith("listener.name.external.") &&
     *         parsedConfigs.containsKey(e.getKey().substring(25))))
     *     // 排除已处理的 Listener 前缀配置：
     *     // ❌ "listener.name.external.ssl.keystore.location"
     *     //    → substring(25) = "ssl.keystore.location" 已存在
     *     // ❌ "listener.name.external.ssl.truststore.location"
     *     //    → substring(25) = "ssl.truststore.location" 已存在
     *
     *     .filter(e -> !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
     *     // 排除 SASL 机制级别的重复配置：
     *     // （这个场景中影响不大）
     *
     *     .forEach(e -> parsedConfigs.put(e.getKey(), e.getValue()));
     *     // 最终添加剩余配置：
     *     // ✅ "listener.name.internal.security.protocol"
     *     // ✅ "listener.name.internal.sasl.mechanism"
     *     // ✅ "listener.name.internal.plain.username"
     *     // ✅ "listener.name.internal.plain.password"
     *     // ✅ "scram-sha-256.password" (来自全局，但值已被 Listener 覆盖)
     * 📊 配置优先级可视化
     * ┌─────────────────────────────────────────────────┐
     * │  EXTERNAL Listener 最终配置                      │
     * ├─────────────────────────────────────────────────┤
     * │ security.protocol     = SASL_SSL                │ ← 全局配置
     * │ sasl.mechanism        = SCRAM-SHA-256           │ ← 全局配置
     * │ ssl.keystore.location = /external/keystore.jks  │ ← Listener 配置 ⭐ (优先级高)
     * │ ssl.truststore.location = /external/truststore.jks │ ← Listener 配置 ⭐
     * │ scram-sha-256.password = default_password       │ ← 全局配置（Listener 未提供）
     * └─────────────────────────────────────────────────┘
     *
     * ┌─────────────────────────────────────────────────┐
     * │  INTERNAL Listener 最终配置                      │
     * ├─────────────────────────────────────────────────┤
     * │ security.protocol     = SASL_PLAINTEXT          │ ← Listener 配置 ⭐
     * │ sasl.mechanism        = PLAIN                   │ ← Listener 配置 ⭐
     * │ plain.username        = internal_user           │ ← Listener 配置 ⭐
     * │ plain.password        = internal_password       │ ← Listener 配置 ⭐
     * └─────────────────────────────────────────────────┘
     *
     * 在 Kafka 多 Listener 配置中，为了避免配置冲突和重复，需要排除以下三类配置：
     *  ✅ 已解析的配置：避免重复
     *  ✅ 已处理的 Listener 前缀配置：这些配置已经去除了前缀并添加到结果中
     *  ✅ 与现有配置冲突的 SASL 机制级别配置：确保 Listener 配置优先级更高
     * 设计意图：
     *  🎯 实现配置的层级继承（Listener > SASL Mechanism > Global）
     *  🎯 提供统一的配置接口给 ChannelBuilder
     *  🎯 支持复杂的多 Listener、多认证机制部署场景
     * 这是 Kafka 为了支持企业级复杂网络环境（内外网隔离、多安全域等）而提供的灵活配置机制！
     * @return a mutable RecordingMap. The elements got from RecordingMap are marked as "used".
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> channelBuilderConfigs(final AbstractConfig config, final ListenerName listenerName) {
        Map<String, Object> parsedConfigs;
        if (listenerName == null)
            parsedConfigs = (Map<String, Object>) config.values();
        else
            parsedConfigs = config.valuesWithPrefixOverride(listenerName.configPrefix());

        config.originals().entrySet().stream()
                /**
                 * 场景 1：排除已经解析过的配置
                 * 排除什么：values() 或 valuesWithPrefixOverride() 已经处理过的配置 避免重复添加
                 * # 假设 listenerName = "EXTERNAL"
                 * # valuesWithPrefixOverride 已经提取了：
                 * listener.name.external.ssl.keystore.location=/path/to/keystore.jks
                 * → 解析为：ssl.keystore.location=/path/to/keystore.jks
                 *
                 * # originals 中还有：
                 * ssl.keystore.location=/path/to/keystore.jks  ← 被排除，因为已存在
                 */
            .filter(e -> !parsedConfigs.containsKey(e.getKey())) // exclude already parsed configs
                /**
                 *  exclude already parsed listener prefix configs
                 * 场景 2：排除已被覆盖的 Listener 前缀配置
                 * 排除什么：
                 *  带有 listener.name.{listener}. 前缀的配置 且去掉前缀后的 key 已经在 parsedConfigs 中存在
                 * // 假设 listenerName = "EXTERNAL"
                 * // configPrefix() = "listener.name.external."
                 *
                 * // originals 中的配置：
                 * listener.name.external.ssl.keystore.location=/external/keystore.jks
                 * listener.name.external.ssl.truststore.location=/external/truststore.jks
                 *
                 * // valuesWithPrefixOverride 处理后：
                 * parsedConfigs = {
                 *     "ssl.keystore.location": "/external/keystore.jks",      // ← 已存在
                 *     "ssl.truststore.location": "/external/truststore.jks"   // ← 已存在
                 * }
                 *
                 * // 现在遍历 originals，遇到：
                 * "listener.name.external.ssl.keystore.location"
                 *   → startsWith("listener.name.external.")? ✅ YES
                 *   → key.substring(...) = "ssl.keystore.location"
                 *   → parsedConfigs.containsKey("ssl.keystore.location")? ✅ YES
                 *   → 被排除 ❌
                 * 为什么排除？
                 *  这些配置已经通过 valuesWithPrefixOverride() 去除了前缀并添加到结果中 再次添加会导致重复
                 *
                 */
            .filter(e -> !(listenerName != null && e.getKey().startsWith(listenerName.configPrefix()) &&
                parsedConfigs.containsKey(e.getKey().substring(listenerName.configPrefix().length()))))
            /**
             * exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
             * 场景 3：排除 SASL 机制级别的配置（当 Listener 配置已存在时）
             * 排除什么：
             *  格式为 {mechanism}.{config} 的配置（如 scram-sha-256.password=xxx）
             *  如果去掉第一个点之前的部分后，key 已经存在于 parsedConfigs 中
             * // 假设 listenerName = "EXTERNAL"
             *
             * // 配置场景 1：Listener 配置优先
             * properties:
             *   # Listener 级别配置
             *   listener.name.external.scram-sha-256.password=external_secret
             *   # SASL 机制级别配置
             *   scram-sha-256.password=global_secret
             *
             * // valuesWithPrefixOverride 处理后：
             * parsedConfigs = {
             *     "scram-sha-256.password": "external_secret"  // ← Listener 配置已存在
             * }
             *
             * // 现在遍历 originals，遇到：
             * "scram-sha-256.password"
             *   → key.indexOf('.') = 14 (第一个点在 "scram-sha-256" 之后)
             *   → key.substring(15) = "password"
             *   → parsedConfigs.containsKey("password")? ❌ NO
             *   → 不会被排除，会添加到结果中
             *
             * // 但如果遇到：
             * "listener.name.external.scram-sha-256.password"
             *   → 已经被场景 2 排除了
             *
             * // 配置场景 2：多层前缀
             * properties:
             *   # SASL 机制配置
             *   sasl.mechanism=SCRAM-SHA-256
             *   scram-sha-256.password=mechanism_password
             *
             *   # Listener 级别的 SASL 配置
             *   listener.name.external.scram-sha-256.password=listener_password
             *
             * // valuesWithPrefixOverride 处理后：
             * parsedConfigs = {
             *     "sasl.mechanism": "SCRAM-SHA-256",
             *     "scram-sha-256.password": "listener_password"  // ← Listener 配置优先级更高
             * }
             *
             * // 现在遍历 originals，检查场景 3：
             * "scram-sha-256.password"
             *   → key.indexOf('.') = 14
             *   → key.substring(15) = "password"
             *   → parsedConfigs.containsKey("password")? ❌ NO
             *   → 不会被排除（这是正确的，因为它本身就是 mechanism 级别的配置）
             *
             * // 但如果是这种特殊情况：
             * "some.prefix.password"  // 假设有个配置长这样
             *   → key.indexOf('.') = 10
             *   → key.substring(11) = "password"
             *   → parsedConfigs.containsKey("password")? ✅ YES（如果已存在）
             *   → 被排除 ❌
             */
            .filter(e -> !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
            .forEach(e -> parsedConfigs.put(e.getKey(), e.getValue()));
        return parsedConfigs;
    }

    private static void requireNonNullMode(ConnectionMode connectionMode, SecurityProtocol securityProtocol) {
        if (connectionMode == null)
            throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`");
    }

    public static KafkaPrincipalBuilder createPrincipalBuilder(Map<String, ?> configs,
                                                               KerberosShortNamer kerberosShortNamer,
                                                               SslPrincipalMapper sslPrincipalMapper) {
        Class<?> principalBuilderClass = (Class<?>) configs.get(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        final KafkaPrincipalBuilder builder;

        if (principalBuilderClass == null || principalBuilderClass == DefaultKafkaPrincipalBuilder.class) {
            builder = new DefaultKafkaPrincipalBuilder(kerberosShortNamer, sslPrincipalMapper);
        } else if (KafkaPrincipalBuilder.class.isAssignableFrom(principalBuilderClass)) {
            builder = (KafkaPrincipalBuilder) Utils.newInstance(principalBuilderClass);
        } else {
            throw new InvalidConfigurationException("Type " + principalBuilderClass.getName() + " is not " +
                    "an instance of " + KafkaPrincipalBuilder.class.getName());
        }

        if (builder instanceof Configurable)
            ((Configurable) builder).configure(configs);

        return builder;
    }

}

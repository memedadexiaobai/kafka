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
package org.apache.kafka.clients;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public final class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(AbstractConfig config) {
        List<String> urls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);// bootstrap.servers
        String clientDnsLookupConfig = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG);// client.dns.lookup
        return parseAndValidateAddresses(urls, clientDnsLookupConfig);
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, String clientDnsLookupConfig) {
        return parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig));
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, ClientDnsLookup clientDnsLookup) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String url : urls) {
            if (url != null && !url.isEmpty()) {
                try {
                    String host = getHost(url);
                    Integer port = getPort(url);
                    if (host == null || port == null)
                        throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

                    /**
                     * 行为：
                     *  立即解析：启动时就查询所有 DNS 记录
                     *  获取规范主机名：通过 getCanonicalHostName() 获取 CNAME
                     *  多 IP 支持：如果一个主机名对应多个 IP，会为每个 IP 创建连接地址
                     *  使用规范名称：用 CNAME 而不是原始主机名创建连接
                     */
                    if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
                        InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                        for (InetAddress inetAddress : inetAddresses) {
                            String resolvedCanonicalName = inetAddress.getCanonicalHostName();
                            InetSocketAddress address = new InetSocketAddress(resolvedCanonicalName, port);
                            if (address.isUnresolved()) {
                                log.warn("Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedCanonicalName, host);
                            } else {
                                addresses.add(address);
                            }
                        }
                    } else {
                        /**
                         * 行为：
                         *  直接使用主机名创建 InetSocketAddress
                         *  延迟解析：直到真正建立连接时才进行 DNS 解析
                         *  每个 IP 只创建一个地址
                         * 适用场景：
                         *  简单的单 IP 环境
                         *  希望减少启动时的 DNS 查询开销
                         */
                        InetSocketAddress address = new InetSocketAddress(host, port);
                        if (address.isUnresolved()) {
                            log.warn("Couldn't resolve server {} from {} as DNS resolution failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
                        } else {
                            addresses.add(address);
                        }
                    }

                } catch (IllegalArgumentException e) {
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                } catch (UnknownHostException e) {
                    throw new ConfigException("Unknown host in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.isEmpty())
            throw new ConfigException("No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    /**
     * Create a new channel builder from the provided configuration.
     *
     * @param config client configs
     * @param time the time implementation
     * @param logContext the logging context
     *
     * @return configured ChannelBuilder based on the configs.
     */
    public static ChannelBuilder createChannelBuilder(AbstractConfig config, Time time, LogContext logContext) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));// security.protocol
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);// sasl.mechanism
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, null,
                clientSaslMechanism, time, true, logContext);
    }

    static List<InetAddress> resolve(String host, HostResolver hostResolver) throws UnknownHostException {
        InetAddress[] addresses = hostResolver.resolve(host);
        List<InetAddress> result = filterPreferredAddresses(addresses);
        if (log.isDebugEnabled())
            log.debug("Resolved host {} as {}", host, result.stream().map(InetAddress::getHostAddress).collect(Collectors.joining(",")));
        return result;
    }

    /**
     * Return a list containing the first address in `allAddresses` and subsequent addresses
     * that are a subtype of the first address.
     *
     * The outcome is that all returned addresses are either IPv4 or IPv6 (InetAddress has two
     * subclasses: Inet4Address and Inet6Address).
     */
    static List<InetAddress> filterPreferredAddresses(InetAddress[] allAddresses) {
        List<InetAddress> preferredAddresses = new ArrayList<>();
        Class<? extends InetAddress> clazz = null;
        for (InetAddress address : allAddresses) {
            if (clazz == null) {
                clazz = address.getClass();
            }
            if (clazz.isInstance(address)) {
                preferredAddresses.add(address);
            }
        }
        return preferredAddresses;
    }

    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    Metadata metadata,
                                                    Sensor throttleTimeSensor,
                                                    ClientTelemetrySender clientTelemetrySender) {
        return createNetworkClient(config,
                config.getString(CommonClientConfigs.CLIENT_ID_CONFIG),
                metrics,
                metricsGroupPrefix,
                logContext,
                apiVersions,
                time,
                maxInFlightRequestsPerConnection,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                metadata,
                null,
                new DefaultHostResolver(),
                throttleTimeSensor,
                clientTelemetrySender);
    }

    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    String clientId,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    int requestTimeoutMs,
                                                    MetadataUpdater metadataUpdater,
                                                    HostResolver hostResolver) {
        return createNetworkClient(config,
                clientId,
                metrics,
                metricsGroupPrefix,
                logContext,
                apiVersions,
                time,
                maxInFlightRequestsPerConnection,
                requestTimeoutMs,
                null,
                metadataUpdater,
                hostResolver,
                null,
                null);
    }

    public static NetworkClient createNetworkClient(AbstractConfig config,
                                                    String clientId,
                                                    Metrics metrics,
                                                    String metricsGroupPrefix,
                                                    LogContext logContext,
                                                    ApiVersions apiVersions,
                                                    Time time,
                                                    int maxInFlightRequestsPerConnection,
                                                    int requestTimeoutMs,
                                                    Metadata metadata,
                                                    MetadataUpdater metadataUpdater,
                                                    HostResolver hostResolver,
                                                    Sensor throttleTimeSensor,
                                                    ClientTelemetrySender clientTelemetrySender) {
        ChannelBuilder channelBuilder = null;
        Selector selector = null;

        try {
            channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            selector = new Selector(config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics,
                    time,
                    metricsGroupPrefix,
                    channelBuilder,
                    logContext);
            return new NetworkClient(metadataUpdater,
                    metadata,
                    selector,
                    clientId,
                    maxInFlightRequestsPerConnection,
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                    requestTimeoutMs,
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext,
                    hostResolver,
                    clientTelemetrySender,
                    MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG))
            );
        } catch (Throwable t) {
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new NetworkClient", t);
        }
    }

    public static <T> List configuredInterceptors(AbstractConfig config,
                                                  String interceptorClassesConfigName,
                                                  Class<T> clazz) {
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        return config.getConfiguredInstances(
                interceptorClassesConfigName,
                clazz,
                Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId));
    }

    public static ClusterResourceListeners configureClusterResourceListeners(List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();

        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        return clusterResourceListeners;
    }
}
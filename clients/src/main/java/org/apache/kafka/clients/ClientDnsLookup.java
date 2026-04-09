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

import java.util.Locale;

/**
 * | 特性 | `use_all_dns_ips` (默认) | `resolve_canonical_bootstrap_servers_only` |
 * |------|------------------------|-------------------------------------------|
 * | **解析时机** | 延迟解析（连接时） | 立即解析（启动时） |
 * | **多 IP 支持** | ❌ 只使用第一个 IP | ✅ 使用所有 IP |
 * | **CNAME 处理** | ❌ 使用原始主机名 | ✅ 使用规范主机名 |
 * | **DNS 缓存** | 依赖 JVM 缓存 | 启动时固定，不随 DNS 变化 |
 * | **启动开销** | 低 | 高（需要多次 DNS 查询） |
 * | **适用场景** | 简单部署 | K8s、云环境、DNS 轮询 |
 */
public enum ClientDnsLookup {
    USE_ALL_DNS_IPS("use_all_dns_ips"),
    RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY("resolve_canonical_bootstrap_servers_only");

    private final String clientDnsLookup;

    ClientDnsLookup(String clientDnsLookup) {
        this.clientDnsLookup = clientDnsLookup;
    }

    @Override
    public String toString() {
        return clientDnsLookup;
    }

    public static ClientDnsLookup forConfig(String config) {
        return ClientDnsLookup.valueOf(config.toUpperCase(Locale.ROOT));
    }
}

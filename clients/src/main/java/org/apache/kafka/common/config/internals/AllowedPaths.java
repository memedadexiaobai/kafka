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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.ConfigException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AllowedPaths {
    private final List<Path> allowedPaths;

    /**
     * Constructs AllowedPaths with a list of Paths retrieved from {@code configValue}.
     * @param configValue {@code allowed.paths} config value which is a string containing comma separated list of paths
     * @throws ConfigException if any of the given paths is not absolute or does not exist.
     */
    public AllowedPaths(String configValue) {
        this.allowedPaths = getAllowedPaths(configValue);
    }

    private List<Path> getAllowedPaths(String configValue) {
        if (configValue != null && !configValue.isEmpty()) {
            List<Path> allowedPaths = new ArrayList<>();

            Arrays.stream(configValue.split(",")).forEach(b -> {
                Path normalisedPath = Paths.get(b).normalize();

                if (!normalisedPath.isAbsolute()) {
                    throw new ConfigException("Path " + normalisedPath + " is not absolute");
                } else if (!Files.exists(normalisedPath)) {
                    throw new ConfigException("Path " + normalisedPath + " does not exist");
                } else {
                    allowedPaths.add(normalisedPath);
                }
            });

            return allowedPaths;
        }

        return null;
    }

    /**
     * Checks if the given {@code path} resides in the configured {@code allowed.paths}.
     * If {@code allowed.paths} is not configured, the given Path is returned as allowed.
     * @param path the Path to check if allowed
     * @return Path that can be accessed or null if the given Path does not reside in the configured {@code allowed.paths}.
     *
     * # 配置文件提供者
     * config.providers=file
     *
     * # 配置 FileConfigProvider 的实现类
     * config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
     *
     * # ⭐ 关键：只允许访问 /etc/kafka/configs 目录
     * config.providers.file.param.allowed.paths=/etc/kafka/configs,/var/lib/kafka/secrets
     *
     * # 使用配置变量
     * ssl.keystore.password=${file:/etc/kafka/configs/keystore.properties:password}
     * 效果：
     * ✅ 允许访问：/etc/kafka/configs/keystore.properties
     * ✅ 允许访问：/var/lib/kafka/secrets/password.properties
     * ❌ 拒绝访问：/etc/passwd（不在白名单）
     * ❌ 拒绝访问：../../../etc/shadow（路径遍历攻击）
     */
    public Path parseUntrustedPath(String path) {
        Path parsedPath = Paths.get(path);

        if (allowedPaths != null) {
            /**
             * normalize方法用于标准化路径，消除路径中的冗余元素（如 . 和 ..），返回一个更简洁、规范的路径表示。
             *
             * // 示例 1：消除 ".." (父目录引用)
             * Path path1 = Paths.get("/etc/kafka/configs/../secrets/password.properties");
             * Path normalized1 = path1.normalize();
             * System.out.println(normalized1);
             * // 输出：/etc/kafka/secrets/password.properties
             *
             * // 示例 2：消除 "." (当前目录)
             * Path path2 = Paths.get("/var/./kafka/./configs");
             * Path normalized2 = path2.normalize();
             * System.out.println(normalized2);
             * // 输出：/var/kafka/configs
             *
             * // 示例 3：混合使用
             * Path path3 = Paths.get("./config/../config/./settings.properties");
             * Path normalized3 = path3.normalize();
             * System.out.println(normalized3);
             * // 输出：config/settings.properties
             *
             * | 原始路径 | normalize() 后 | 说明 |
             * |---------|---------------|------|
             * | `/a/b/c/./d` | `/a/b/c/d` | 移除 `.` |
             * | `/a/b/../c` | `/a/c` | 解析 `..` |
             * | `/a/b/../../c` | `/c` | 多重 `..` |
             * | `/a/./b/./c` | `/a/b/c` | 多个 `.` |
             * | `./a/b/../c` | `./a/c` | 相对路径也适用 |
             * | `/a/b/c` | `/a/b/c` | 无变化 |
             *
             * normalize() 方法的核心价值：
             * ┌─────────────────────────────────────────────┐
             * │ 1. 路径简化                                 │
             * │    - 移除 "." (当前目录)                     │
             * │    - 解析 ".." (父目录)                      │
             * ├─────────────────────────────────────────────┤
             * │ 2. 安全防护                                 │
             * │    - 防止路径遍历攻击                        │
             * │    - 暴露真实目标路径                        │
             * ├─────────────────────────────────────────────┤
             * │ 3. 纯字符串操作                             │
             * │    - 不检查路径是否存在                      │
             * │    - 不访问文件系统                          │
             * ├─────────────────────────────────────────────┤
             * │ 4. 跨平台兼容                               │
             * │    - Windows: C:\a\b\..\c → C:\c            │
             * │    - Linux: /a/b/../c → /a/c                │
             * └─────────────────────────────────────────────┘
             */
            Path normalisedPath = parsedPath.normalize();
            // 检查是否在允许的目录内
            long allowed = allowedPaths.stream().filter(normalisedPath::startsWith).count();
            if (allowed == 0) {
                return null; // ❌ 不在白名单中，拒绝访问
            }
            return normalisedPath;
        }

        return parsedPath; // 未配置限制，直接返回
    }
}

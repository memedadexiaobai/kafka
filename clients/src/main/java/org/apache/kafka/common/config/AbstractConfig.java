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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Configs for which values have been requested, used to detect unused configs.
     * This set must be concurrent modifiable and iterable. It will be modified
     * when directly accessed or as a result of RecordingMap access.
     */
    private final Set<String> used = ConcurrentHashMap.newKeySet();

    /* the original values passed in by the user 用户传入的原始值 */
    private final Map<String, ?> originals;

    /* the parsed values */
    private final Map<String, Object> values;

    private final ConfigDef definition;

    public static final String AUTOMATIC_CONFIG_PROVIDERS_PROPERTY = "org.apache.kafka.automatic.config.providers";

    public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";

    private static final String CONFIG_PROVIDERS_PARAM = ".param.";

    /**
     * Construct a configuration with a ConfigDef and the configuration properties, which can include properties
     * for zero or more {@link ConfigProvider} that will be used to resolve variables in configuration property
     * values.
     * <p>
     * The originals is a name-value pair configuration properties and optional config provider configs. The
     * value of the configuration can be a variable as defined below or the actual value. This constructor will
     * first instantiate the ConfigProviders using the config provider configs, then it will find all the
     * variables in the values of the originals configurations, attempt to resolve the variables using the named
     * ConfigProviders, and then parse and validate the configurations.
     * <p>
     * ConfigProvider configs can be passed either as configs in the originals map or in the separate
     * configProviderProps map. If config providers properties are passed in the configProviderProps any config
     * provider properties in originals map will be ignored. If ConfigProvider properties are not provided, the
     * constructor will skip the variable substitution step and will simply validate and parse the supplied
     * configuration.
     * <p>
     * The "{@code config.providers}" configuration property and all configuration properties that begin with the
     * "{@code config.providers.}" prefix are reserved. The "{@code config.providers}" configuration property
     * specifies the names of the config providers, and properties that begin with the "{@code config.providers..}"
     * prefix correspond to the properties for that named provider. For example, the "{@code config.providers..class}"
     * property specifies the name of the {@link ConfigProvider} implementation class that should be used for
     * the provider.
     * <p>
     * The keys for ConfigProvider configs in both originals and configProviderProps will start with the above
     * mentioned "{@code config.providers.}" prefix.
     * <p>
     * Variables have the form "${providerName:[path:]key}", where "providerName" is the name of a ConfigProvider,
     * "path" is an optional string, and "key" is a required string. This variable is resolved by passing the "key"
     * and optional "path" to a ConfigProvider with the specified name, and the result from the ConfigProvider is
     * then used in place of the variable. Variables that cannot be resolved by the AbstractConfig constructor will
     * be left unchanged in the configuration.
     *
     * @param definition          the definition of the configurations; may not be null
     * @param originals           the configuration properties plus any optional config provider properties;
     * @param configProviderProps the map of properties of config providers which will be instantiated by
     *                            the constructor to resolve any variables in {@code originals}; may be null or empty
     * @param doLog               whether the configurations should be logged
     *
     * 用户输入配置
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 1 步：类型转换                          │
     * │ Utils.castToStringObjectMap(originals)   │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 2 步：解析变量（核心）⭐                │
     * │ resolveConfigVariables(...)               │
     * │ - 实例化 ConfigProvider                   │
     * │ - 调用 provider.get() 获取值              │
     * │ - 替换 ${file:/path:key} 等变量          │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 3 步：第一次解析验证                    │
     * │ definition.parse(this.originals)          │
     * │ - 验证配置项是否合法                      │
     * │ - 应用默认值                              │
     * │ - 类型转换                                │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 4 步：后处理（子类扩展点）             │
     * │ postProcessParsedConfig(...)              │
     * │ - 允许子类修改已解析的配置                │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 5 步：合并后处理的配置                  │
     * │ this.values.putAll(configUpdates)         │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 6 步：第二次解析验证                    │
     * │ definition.parse(this.values)             │
     * │ - 确保后处理的配置也合法                  │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 7 步：保存定义                          │
     * │ this.definition = definition              │
     * └───────────────────────────────────────────┘
     *     ↓
     * ┌───────────────────────────────────────────┐
     * │ 第 8 步：可选日志输出                      │
     * │ if (doLog) logAll()                       │
     * └───────────────────────────────────────────┘
     *     ↓
     * 构造完成
     *
     * 📊 完整数据流图:
     * ┌─────────────────────────────────────────────────────────┐
     * │ 用户输入                                                 │
     * │ originals = {                                           │
     * │   "bootstrap.servers": "localhost:9092",               │
     * │   "ssl.password": "${file:/etc/secrets:pwd}"           │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     *                             ↓
     * ┌─────────────────────────────────────────────────────────┐
     * │ configProviderProps                                      │
     * │ {                                                        │
     * │   "config.providers": "file",                           │
     * │   "config.providers.file.class": "...FileConfig...",   │
     * │   "config.providers.file.param.allowed.paths": "/etc"  │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     *                             ↓
     *               resolveConfigVariables()
     *                             ↓
     * ┌─────────────────────────────────────────────────────────┐
     * │ this.originals (变量已替换)                              │
     * │ {                                                        │
     * │   "bootstrap.servers": "localhost:9092",               │
     * │   "ssl.password": "secret_123"  ← 已解析               │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     *                             ↓
     *               definition.parse()
     *                             ↓
     * ┌─────────────────────────────────────────────────────────┐
     * │ this.values (类型转换 + 默认值)                          │
     * │ {                                                        │
     * │   "bootstrap.servers": "localhost:9092",               │
     * │   "ssl.password": "secret_123",                        │
     * │   "request.timeout.ms": 30000  ← 默认值                │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     *                             ↓
     *           postProcessParsedConfig()
     *                             ↓
     * ┌─────────────────────────────────────────────────────────┐
     * │ configUpdates (子类修改)                                 │
     * │ {                                                        │
     * │   "acks": "all"  ← 强制设置                            │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     *                             ↓
     *               putAll + parse()
     *                             ↓
     * ┌─────────────────────────────────────────────────────────┐
     * │ 最终 this.values                                         │
     * │ {                                                        │
     * │   "bootstrap.servers": "localhost:9092",               │
     * │   "ssl.password": "secret_123",                        │
     * │   "request.timeout.ms": 30000,                         │
     * │   "acks": "all"                                         │
     * │ }                                                        │
     * └─────────────────────────────────────────────────────────┘
     * 核心价值：
     * • 支持外部化配置（文件、Vault、环境变量）
     * • 严格的两次验证机制
     * • 提供子类扩展点
     * • 统一的类型转换和验证
     */
    @SuppressWarnings({"this-escape"})
    public AbstractConfig(ConfigDef definition,   // 配置定义（规则
                          Map<?, ?> originals, // 原始配置（用户输入）
                          Map<String, ?> configProviderProps,// ConfigProvider 配置
                          boolean doLog) {// 是否打印日志
        //将 Map<?, ?> 转换为 Map<String, Object>
        // 确保 key 都是 String 类型
        Map<String, Object> originalMap = Utils.castToStringObjectMap(originals);

        /**
         * // originalMap（用户原始配置）
         * {
         *     "bootstrap.servers": "localhost:9092",
         *     "ssl.keystore.password": "${file:/etc/secrets:password}",
         *     "sasl.password": "${vault:/secret/kafka:password}"
         * }
         *
         * // configProviderProps（ConfigProvider 配置）
         * {
         *     "config.providers": "file,vault",
         *     "config.providers.file.class": "org.apache.kafka...FileConfigProvider",
         *     "config.providers.file.param.allowed.paths": "/etc/secrets",
         *     "config.providers.vault.class": "com.example.VaultConfigProvider",
         *     "config.providers.vault.param.vault.url": "https://vault:8200"
         * }
         * resolveConfigVariables() 方法内部：
         * ┌─────────────────────────────────────────────┐
         * │ 1. 提取包含变量的配置项                     │
         * │    ${file:/etc/secrets:password}            │
         * │    ${vault:/secret/kafka:password}          │
         * ├─────────────────────────────────────────────┤
         * │ 2. 实例化 ConfigProvider                    │
         * │    - FileConfigProvider 实例                │
         * │    - VaultConfigProvider 实例               │
         * ├─────────────────────────────────────────────┤
         * │ 3. 调用 provider.get() 获取实际值           │
         * │    - FileConfigProvider.get("/etc/secrets", ["password"])
         * │      → returns {"password": "file_secret_123"}
         * │    - VaultConfigProvider.get("/secret/kafka", ["password"])
         * │      → returns {"password": "vault_secret_456"}
         * ├─────────────────────────────────────────────┤
         * │ 4. 替换变量                                 │
         * │    ${file:/etc/secrets:password} → "file_secret_123"
         * │    ${vault:/secret/kafka:password} → "vault_secret_456"
         * └─────────────────────────────────────────────┘
         * this.originals = {
         *     "bootstrap.servers": "localhost:9092",
         *     "ssl.keystore.password": "file_secret_123",      // ← 已替换
         *     "sasl.password": "vault_secret_456"              // ← 已替换
         * }
         */
        this.originals = resolveConfigVariables(configProviderProps, originalMap);
        /**
         * 作用：
         *  验证配置项合法性 - 检查配置名是否在 ConfigDef 中定义
         *  类型转换 - String → Integer/Long/Boolean 等
         *  应用默认值 - 对于未提供的配置项使用默认值
         *  验证约束 - 检查值是否在允许范围内
         */
        this.values = definition.parse(this.originals);
        // 设计意图：
        //提供给子类一个钩子方法来修改已解析的配置
        //实现「二次 defaults」逻辑 默认不做任何修改
        Map<String, Object> configUpdates = postProcessParsedConfig(Collections.unmodifiableMap(this.values));
        // 合并后处理的配置
        this.values.putAll(configUpdates);
        /**
         * 第二次解析验证
         * 为什么需要第二次？
         * 因为子类可能在 postProcessParsedConfig() 中添加了新的配置，需要再次验证
          */
        definition.parse(this.values);
        /**
         * 作用：
         *  保存 ConfigDef 引用，供后续使用
         *  用于 documentationOf(), values(), originals() 等方法
          */
        this.definition = definition;
        if (doLog)
            /**
             * AbstractConfig values:
             *    acks = all
             *    bootstrap.servers = localhost:9092
             *    compression.type = none
             *    request.timeout.ms = 60000
             */
            logAll();
    }

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigProvider}
     * that will be used to resolve variables in configuration property values.
     *
     * @param definition the definition of the configurations; may not be null
     * @param originals  the configuration properties plus any optional config provider properties; may not be null
     */
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        this(definition, originals, Collections.emptyMap(), true);
    }

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigProvider}
     * that will be used to resolve variables in configuration property values.
     *
     * @param definition the definition of the configurations; may not be null
     * @param originals  the configuration properties plus any optional config provider properties; may not be null
     * @param doLog      whether the configurations should be logged
     */
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        this(definition, originals, Collections.emptyMap(), doLog);

    }

    /**
     * Called directly after user configs got parsed (and thus default values got set).
     * This allows to change default values for "secondary defaults" if required.
     *
     * @param parsedValues unmodifiable map of current configuration
     * @return a map of updates that should be applied to the configuration (will be validated to prevent bad updates)
     */
    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        return Collections.emptyMap();
    }

    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        used.add(key);
        return values.get(key);
    }

    public void ignore(String key) {
        used.add(key);
    }

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        final String res = (String) get(key);
        return res == null ? res : res.trim();
    }

    public ConfigDef.Type typeOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.type;
    }

    public String documentationOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.documentation;
    }

    public Password getPassword(String key) {
        return (Password) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public Set<String> unused() {
        Set<String> keys = new HashSet<>(originals.keySet());
        keys.removeAll(used);
        return keys;
    }

    public Map<String, Object> originals() {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        return copy;
    }

    public Map<String, Object> originals(Map<String, Object> configOverrides) {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        copy.putAll(configOverrides);
        return copy;
    }

    /**
     * Get all the original settings, ensuring that all values are of type String.
     *
     * @return the original settings
     * @throws ClassCastException if any of the values are not strings
     */
    public Map<String, String> originalsStrings() {
        Map<String, String> copy = new RecordingMap<>();
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (!(entry.getValue() instanceof String))
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey() +
                        ": " + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
            copy.put(entry.getKey(), (String) entry.getValue());
        }
        return copy;
    }

    /**
     * Gets all original settings with the given prefix, stripping the prefix before adding it to the output.
     *
     * @param prefix the prefix to use as a filter
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix) {
        return originalsWithPrefix(prefix, true);
    }

    /**
     * Gets all original settings with the given prefix.
     *
     * @param prefix the prefix to use as a filter
     * @param strip  strip the prefix before adding to the output if set true
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix, boolean strip) {
        Map<String, Object> result = new RecordingMap<>(prefix, false);
        result.putAll(Utils.entriesWithPrefix(originals, prefix, strip));
        return result;
    }

    /**
     * Put all keys that do not start with {@code prefix} and their parsed values in the result map and then
     * put all the remaining keys with the prefix stripped and their parsed values in the result map.
     * <p>
     * This is useful if one wants to allow prefixed configs to override default ones.
     * <p>
     * Two forms of prefixes are supported:
     * <ul>
     *     <li>listener.name.{listenerName}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     *         the key `some.prop` with the value parsed using the definition of `some.prop` is returned.</li>
     *     <li>listener.name.{listenerName}.{mechanism}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     *         the key `{mechanism}.some.prop` with the value parsed using the definition of `some.prop` is returned.
     *          This is used to provide per-mechanism configs for a broker listener (e.g sasl.jaas.config)</li>
     * </ul>
     * </p>
     */
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                String keyWithNoPrefix = entry.getKey().substring(prefix.length());
                ConfigDef.ConfigKey configKey = definition.configKeys().get(keyWithNoPrefix);
                if (configKey != null)
                    result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                else {
                    String keyWithNoSecondaryPrefix = keyWithNoPrefix.substring(keyWithNoPrefix.indexOf('.') + 1);
                    configKey = definition.configKeys().get(keyWithNoSecondaryPrefix);
                    if (configKey != null)
                        result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                }
            }
        }
        return result;
    }

    /**
     * If at least one key with {@code prefix} exists, all prefixed values will be parsed and put into map.
     * If no value with {@code prefix} exists all unprefixed values will be returned.
     * <p>
     * This is useful if one wants to allow prefixed configs to override default ones, but wants to use either
     * only prefixed configs or only regular configs, but not mix them.
     */
    public Map<String, Object> valuesWithPrefixAllOrNothing(String prefix) {
        Map<String, Object> withPrefix = originalsWithPrefix(prefix, true);

        if (withPrefix.isEmpty()) {
            return new RecordingMap<>(values(), "", true);
        } else {
            Map<String, Object> result = new RecordingMap<>(prefix, true);

            for (Map.Entry<String, ?> entry : withPrefix.entrySet()) {
                ConfigDef.ConfigKey configKey = definition.configKeys().get(entry.getKey());
                if (configKey != null)
                    result.put(entry.getKey(), definition.parseValue(configKey, entry.getValue(), true));
            }

            return result;
        }
    }

    public Map<String, ?> values() {
        return new RecordingMap<>(values);
    }

    public Map<String, ?> nonInternalValues() {
        Map<String, Object> nonInternalConfigs = new RecordingMap<>();
        values.forEach((key, value) -> {
            ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
            if (configKey == null || !configKey.internalConfig) {
                nonInternalConfigs.put(key, value);
            }
        });
        return nonInternalConfigs;
    }

    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);

        for (Map.Entry<String, Object> entry : new TreeMap<>(this.values).entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    /**
     * Info level log for any unused configurations
     */
    public void logUnused() {
        Set<String> unusedKeys = unused();
        if (!unusedKeys.isEmpty()) {
            log.info("These configurations '{}' were supplied but are not used yet.", unusedKeys);
        }
    }

    private <T> T getConfiguredInstance(Object klass, Class<T> t, Map<String, Object> configPairs) {
        if (klass == null)
            return null;
        Object o;

        if (klass instanceof String) {
            try {
                o = Utils.newInstance((String) klass, t);
            } catch (ClassNotFoundException e) {
                throw new KafkaException("Class " + klass + " cannot be found", e);
            }
        } else if (klass instanceof Class<?>) {
            o = Utils.newInstance((Class<?>) klass);
        } else
            throw new KafkaException("Unexpected element of type " + klass.getClass().getName() + ", expected String or Class");
        try {
            if (!t.isInstance(o))
                throw new KafkaException(klass + " is not an instance of " + t.getName());
            if (o instanceof Configurable)
                ((Configurable) o).configure(configPairs);
        } catch (Exception e) {
            maybeClose(o, "AutoCloseable object constructed and configured during failed call to getConfiguredInstance");
            throw e;
        }
        return t.cast(o);
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     *
     * @param key The configuration key for the class
     * @param t   The interface the class should implement
     * @return A configured instance of the class
     */
    public <T> T getConfiguredInstance(String key, Class<T> t) {
        return getConfiguredInstance(key, t, Collections.emptyMap());
    }

    /**
     * Get a configured instance of the give class specified by the given configuration key. If the object implements
     * Configurable configure it using the configuration.
     *
     * @param key             The configuration key for the class
     * @param t               The interface the class should implement
     * @param configOverrides override origin configs
     * @return A configured instance of the class
     */
    public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
        Class<?> c = getClass(key);

        return getConfiguredInstance(c, t, originals(configOverrides));
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param key The configuration key for the class
     * @param t   The interface the class should implement
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
        return getConfiguredInstances(key, t, Collections.emptyMap());
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param key             The configuration key for the class
     * @param t               The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
        return getConfiguredInstances(getList(key), t, configOverrides);
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param classNames      The list of class names of the instances to create
     * @param t               The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(List<String> classNames, Class<T> t, Map<String, Object> configOverrides) {
        List<T> objects = new ArrayList<>();
        if (classNames == null)
            return objects;
        Map<String, Object> configPairs = originals();
        configPairs.putAll(configOverrides);

        try {
            for (Object klass : classNames) {
                Object o = getConfiguredInstance(klass, t, configPairs);
                objects.add(t.cast(o));
            }
        } catch (Exception e) {
            for (Object object : objects) {
                maybeClose(object, "AutoCloseable object constructed and configured during failed call to getConfiguredInstances");
            }
            throw e;
        }
        return objects;
    }

    private static void maybeClose(Object object, String name) {
        if (object instanceof AutoCloseable) {
            Utils.closeQuietly((AutoCloseable) object, name);
        }
    }

    private Map<String, String> extractPotentialVariables(Map<?, ?> configMap) {
        // Variables are tuples of the form "${providerName:[path:]key}". From the configMap we extract the subset of configs with string
        // values as potential variables.
        Map<String, String> configMapAsString = new HashMap<>();
        for (Map.Entry<?, ?> entry : configMap.entrySet()) {
            if (entry.getValue() instanceof String)
                configMapAsString.put((String) entry.getKey(), (String) entry.getValue());
        }

        return configMapAsString;
    }

    /**
     * Instantiates given list of config providers and fetches the actual values of config variables from the config providers.
     * returns a map of config key and resolved values.
     *
     * @param configProviderProps The map of config provider configs
     * @param originals           The map of raw configs.
     * @return map of resolved config variable.
     */
    private Map<String, ?> resolveConfigVariables(Map<String, ?> configProviderProps, Map<String, Object> originals) {
        Map<String, String> providerConfigString;
        Map<String, ?> configProperties;
        Predicate<String> classNameFilter;
        Map<String, Object> resolvedOriginals = new HashMap<>();
        // As variable configs are strings, parse the originals and obtain the potential variable configs.
        Map<String, String> indirectVariables = extractPotentialVariables(originals);//把value是字符串的先提取出来

        resolvedOriginals.putAll(originals);
        if (configProviderProps == null || configProviderProps.isEmpty()) {
            providerConfigString = indirectVariables;
            configProperties = originals;
            classNameFilter = automaticConfigProvidersFilter();
        } else {
            providerConfigString = extractPotentialVariables(configProviderProps);
            configProperties = configProviderProps;
            classNameFilter = ignored -> true;
        }
        Map<String, ConfigProvider> providers = instantiateConfigProviders(providerConfigString, configProperties, classNameFilter);

        if (!providers.isEmpty()) {
            ConfigTransformer configTransformer = new ConfigTransformer(providers);
            ConfigTransformerResult result = configTransformer.transform(indirectVariables);
            if (!result.data().isEmpty()) {
                resolvedOriginals.putAll(result.data());
            }
        }
        providers.values().forEach(x -> Utils.closeQuietly(x, "config provider"));

        return new ResolvingMap<>(resolvedOriginals, originals);
    }

    private Predicate<String> automaticConfigProvidersFilter() {
        /**
         * org.apache.kafka.automatic.config.providers
         * Kafka 支持从外部来源（如文件、环境变量、Vault 等）动态加载配置，
         * 这通过 ConfigProvider 机制实现。但为了防止恶意配置提供者被加载，
         * Kafka 需要一个白名单机制来控制哪些 ConfigProvider 可以被自动加载。
         *
         * FileConfigProvider：从文件读取配置 如：${file:/path/to/config:key}
         * EnvVarConfigProvider：从环境变量读取 如：${env:MY_PASSWORD}
         * DirectoryConfigProvider：从目录文件读取 如：${dir:/path:filename}
         *
         * 比如：
         * # 只允许指定的 ConfigProvider
         * java -Dorg.apache.kafka.automatic.config.providers=\
         *   org.apache.kafka.common.config.provider.FileConfigProvider,\
         *   org.apache.kafka.common.config.provider.EnvVarConfigProvider \
         *   -jar kafka.jar
         * # 运维人员在启动脚本中明确指定可信的提供者
         * JAVA_OPTS="-Dorg.apache.kafka.automatic.config.providers=\
         *   org.apache.kafka.common.config.provider.FileConfigProvider,\
         *   org.apache.kafka.common.config.provider.EnvVarConfigProvider"
         */
        String systemProperty = System.getProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY);
        if (systemProperty == null) {
            // 未设置时：允许所有 ConfigProvider
            return ignored -> true;
        } else {
            // 已设置时：只允许白名单中的 ConfigProvider
            return Arrays.stream(systemProperty.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet())::contains;
        }
    }

    private Map<String, Object> configProviderProperties(String configProviderPrefix, Map<String, ?> providerConfigProperties) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, ?> entry : providerConfigProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(configProviderPrefix) && key.length() > configProviderPrefix.length()) {
                result.put(key.substring(configProviderPrefix.length()), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Instantiates and configures the ConfigProviders. The config providers configs are defined as follows:
     * config.providers : A comma-separated list of names for providers.
     * config.providers.{name}.class : The Java class name for a provider.
     * config.providers.{name}.param.{param-name} : A parameter to be passed to the above Java class on initialization.
     * returns a map of config provider name and its instance.
     *
     * @param indirectConfigs          The map of potential variable configs
     * @param providerConfigProperties The map of config provider configs
     * @param classNameFilter          Filter for config provider class names
     * @return map of config provider name and its instance.
     *
     * 这个方法的作用是：实例化并配置 ConfigProvider（配置提供者），用于从外部源（文件、环境变量等）动态加载配置值。
     *
     * # ========== 步骤 1：声明使用哪些 ConfigProvider ==========
     * # 格式：config.providers = <provider-name1>,<provider-name2>,...
     * config.providers = file,vault
     *
     * # ========== 步骤 2：为每个 Provider 指定实现类 ==========
     * # 格式：config.providers.<name>.class = <full-class-name>
     * config.providers.file.class = org.apache.kafka.common.config.provider.FileConfigProvider
     * config.providers.vault.class = com.example.security.VaultConfigProvider
     *
     * # ========== 步骤 3：配置 Provider 的参数 ==========
     * # 格式：config.providers.<name>.param.<param-name> = <param-value>
     *
     * # file provider 的参数
     * config.providers.file.param.allowed.paths = /etc/kafka/configs,/var/lib/kafka/secrets
     *
     * # vault provider 的参数
     * config.providers.vault.param.vault.url = https://vault.example.com:8200
     * config.providers.vault.param.vault.token = s.xxxxx
     * config.providers.vault.param.vault.path = secret/kafka
     *
     * # ========== 步骤 4：在配置中使用变量 ==========
     * # 格式：${<provider-name>:<path>:<key>}
     *
     * # 使用 file provider 读取密钥库密码
     * ssl.keystore.password = ${file:/etc/kafka/configs/keystore.properties:keystore_password}
     *
     * # 使用 vault provider 读取 SASL 密码
     * sasl.password = ${vault:/secret/kafka:sasl_password}
     *
     * # 可以混合使用多个 provider
     * ssl.truststore.password = ${file:/var/lib/kafka/secrets/truststore.properties:truststore_password}
     */
    private Map<String, ConfigProvider> instantiateConfigProviders(
            Map<String, String> indirectConfigs, // 包含变量引用的原始配置
            Map<String, ?> providerConfigProperties, // ConfigProvider 的配置属性
            Predicate<String> classNameFilter //类名过滤器（安全白名单）
    ) {
        // config.providers=file
        final String configProviders = indirectConfigs.get(CONFIG_PROVIDERS_CONFIG);// config.providers

        if (configProviders == null || configProviders.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> providerMap = new HashMap<>();

        for (String provider : configProviders.split(",")) {
            /**
             *  file 的实现类:
             *    config.providers.file.class = #{@link org.apache.kafka.common.config.provider.FileConfigProvider}
             *  vault 的实现类:
             *   config.providers.vault.class =  #{@link com.example.security.VaultConfigProvider }
             *  env 的实现类:
             *   config.providers.env.class = #{@link org.apache.kafka.common.config.provider.EnvVarConfigProvider}
             */
            String providerClass = providerClassProperty(provider);
            if (indirectConfigs.containsKey(providerClass)) {
                String providerClassName = indirectConfigs.get(providerClass);
                if (classNameFilter.test(providerClassName)) {
                    providerMap.put(provider, providerClassName);
                } else {
                    throw new ConfigException(providerClassName + " is not allowed. Update System property '"
                            + AUTOMATIC_CONFIG_PROVIDERS_PROPERTY + "' to allow " + providerClassName);
                }
            }
        }
        // Instantiate Config Providers
        Map<String, ConfigProvider> configProviderInstances = new HashMap<>();
        for (Map.Entry<String, String> entry : providerMap.entrySet()) {
            try {
                /**
                 * # file provider 的参数
                 * config.providers.file.param.allowed.paths = /etc/kafka/configs
                 *
                 * # vault provider 的参数
                 * config.providers.vault.param.vault.url = https://vault.example.com
                 * config.providers.vault.param.vault.token = mytoken
                 * config.providers.vault.param.vault.path = secret/kafka
                 *
                 * # env provider 的参数（通常不需要）
                 * # config.providers.env.param.xxx = ...
                 */
                String prefix = CONFIG_PROVIDERS_CONFIG + "." + entry.getKey() + CONFIG_PROVIDERS_PARAM;
                Map<String, ?> configProperties = configProviderProperties(prefix, providerConfigProperties);
                ConfigProvider provider = Utils.newInstance(entry.getValue(), ConfigProvider.class);
                provider.configure(configProperties);
                configProviderInstances.put(entry.getKey(), provider);
            } catch (ClassNotFoundException e) {
                log.error("Could not load config provider class " + entry.getValue(), e);
                throw new ConfigException(providerClassProperty(entry.getKey()), entry.getValue(), "Could not load config provider class or one of its dependencies");
            }
        }

        return configProviderInstances;
    }

    private static String providerClassProperty(String providerName) {
        return String.format("%s.%s.class", CONFIG_PROVIDERS_CONFIG, providerName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractConfig that = (AbstractConfig) o;

        return originals.equals(that.originals);
    }

    @Override
    public int hashCode() {
        return originals.hashCode();
    }

    /**
     * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes a `Map` instead
     * of an `AbstractConfig` and we can't change that without breaking public API like `Partitioner`.
     */
    private class RecordingMap<V> extends HashMap<String, V> {

        private final String prefix;
        private final boolean withIgnoreFallback;

        RecordingMap() {
            this("", false);
        }

        RecordingMap(String prefix, boolean withIgnoreFallback) {
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        RecordingMap(Map<String, ? extends V> m) {
            this(m, "", false);
        }

        RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
            super(m);
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        @Override
        public V get(Object key) {
            if (key instanceof String) {
                String stringKey = (String) key;
                String keyWithPrefix;
                if (prefix.isEmpty()) {
                    keyWithPrefix = stringKey;
                } else {
                    keyWithPrefix = prefix + stringKey;
                }
                ignore(keyWithPrefix);
                if (withIgnoreFallback)
                    ignore(stringKey);
            }
            return super.get(key);
        }
    }

    /**
     * ResolvingMap keeps a track of the original map instance and the resolved configs.
     * The originals are tracked in a separate nested map and may be a `RecordingMap`; thus
     * any access to a value for a key needs to be recorded on the originals map.
     * The resolved configs are kept in the inherited map and are therefore mutable, though any
     * mutations are not applied to the originals.
     */
    private static class ResolvingMap<V> extends HashMap<String, V> {

        private final Map<String, ?> originals;

        ResolvingMap(Map<String, ? extends V> resolved, Map<String, ?> originals) {
            super(resolved);
            this.originals = Collections.unmodifiableMap(originals);
        }

        @Override
        public V get(Object key) {
            if (key instanceof String && originals.containsKey(key)) {
                // Intentionally ignore the result; call just to mark the original entry as used
                originals.get(key);
            }
            // But always use the resolved entry
            return super.get(key);
        }
    }
}

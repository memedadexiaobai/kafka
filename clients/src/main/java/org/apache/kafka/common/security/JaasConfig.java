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
package org.apache.kafka.common.security;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

/**
 * JAAS configuration parser that constructs a JAAS configuration object with a single
 * login context from the Kafka configuration option {@link SaslConfigs#SASL_JAAS_CONFIG}.
 * <p/>
 * JAAS configuration file format is described <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html">here</a>.
 * The format of the property value is:
 * <pre>
 * {@code
 *   <loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;
 * }
 * </pre>
 *
 * 该类解析Jaas文件中的loginModule信息转为AppConfigurationEntry
 * loginContextName：代表当前上下文 如KafkaServer
 * configEntries：存储对应的LoginModule信息，可能存在多个LoginModule 这里用list存储
 */
class JaasConfig extends Configuration {

    private final String loginContextName;
    private final List<AppConfigurationEntry> configEntries;

    /**
     * 比如 "KafkaServer { test.LoginModule required; };",
     * @param loginContextName KafkaServer
     * @param jaasConfigParams  test.LoginModule required; 解析后转为 AppConfigurationEntry
     */
    public JaasConfig(String loginContextName, String jaasConfigParams) {
        /**
         * StreamTokenizer 是 Java 标准库中的一个工具类，用于将输入流中的文本分解成“令牌”（tokens）。
         * 它通常用于解析结构化的文本数据，如配置文件、脚本语言等
         *
         * 主要方法：
         * int nextToken()：
         *  读取下一个令牌，并返回其类型。常见的返回值有：
         *      TT_EOF：表示已到达输入流的末尾。
         *      TT_EOL：表示已到达行尾。
         *      TT_NUMBER：表示读取到一个数字。
         *      TT_WORD：表示读取到一个单词。
         *      其他值：表示读取到的单个字符。
         *  读取到的数字可以通过 nval 属性获取，读取到的单词或字符串可以通过 sval 属性获取。
         * void resetSyntax()：重置默认的语法，使所有字符都被视为普通字符。
         * void wordChars(int low, int hi)：将指定范围内的字符标记为单词字符。例如，wordChars('a', 'z') 表示小写字母都是单词字符。
         * void whitespaceChars(int low, int hi)：将指定范围内的字符标记为空白字符。例如，whitespaceChars(' ', ' ') 表示空格是空白字符。
         * void ordinaryChar(int ch)：将指定字符标记为普通字符，不再被视为特殊字符。
         * void commentChar(int ch)：将指定字符标记为注释字符。例如，commentChar('#') 表示 # 后面的内容是注释。
         * void quoteChar(int ch)：将指定字符标记为引号字符，用于包围字符串。例如，quoteChar('"') 表示双引号可以包围字符串。
         * void parseNumbers()：启用数字解析，使 StreamTokenizer 能够识别并解析数字。
         */
        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(jaasConfigParams));
        tokenizer.slashSlashComments(true);
        tokenizer.slashStarComments(true);
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('$', '$');

        try {
            configEntries = new ArrayList<>();
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                configEntries.add(parseAppConfigurationEntry(tokenizer));
            }
            if (configEntries.isEmpty())
                throw new IllegalArgumentException("Login module not specified in JAAS config");

            this.loginContextName = loginContextName;

        } catch (IOException e) {
            throw new KafkaException("Unexpected exception while parsing JAAS config");
        }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (this.loginContextName.equals(name))
            return configEntries.toArray(new AppConfigurationEntry[0]);
        else
            return  null;
    }

    private LoginModuleControlFlag loginModuleControlFlag(String flag) {
        if (flag == null)
            throw new IllegalArgumentException("Login module control flag is not available in the JAAS config");

        LoginModuleControlFlag controlFlag;
        switch (flag.toUpperCase(Locale.ROOT)) {
            case "REQUIRED":
                controlFlag = LoginModuleControlFlag.REQUIRED;
                break;
            case "REQUISITE":
                controlFlag = LoginModuleControlFlag.REQUISITE;
                break;
            case "SUFFICIENT":
                controlFlag = LoginModuleControlFlag.SUFFICIENT;
                break;
            case "OPTIONAL":
                controlFlag = LoginModuleControlFlag.OPTIONAL;
                break;
            default:
                throw new IllegalArgumentException("Invalid login module control flag '" + flag + "' in JAAS config");
        }
        return controlFlag;
    }

    private AppConfigurationEntry parseAppConfigurationEntry(StreamTokenizer tokenizer) throws IOException {
        String loginModule = tokenizer.sval;
        if (tokenizer.nextToken() == StreamTokenizer.TT_EOF)
            throw new IllegalArgumentException("Login module control flag not specified in JAAS config");
        LoginModuleControlFlag controlFlag = loginModuleControlFlag(tokenizer.sval);
        Map<String, String> options = new HashMap<>();
        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF && tokenizer.ttype != ';') {
            String key = tokenizer.sval;
            if (tokenizer.nextToken() != '=' || tokenizer.nextToken() == StreamTokenizer.TT_EOF || tokenizer.sval == null)
                throw new IllegalArgumentException("Value not specified for key '" + key + "' in JAAS config");
            String value = tokenizer.sval;
            options.put(key, value);
        }
        if (tokenizer.ttype != ';')
            throw new IllegalArgumentException("JAAS config entry not terminated by semi-colon");
        return new AppConfigurationEntry(loginModule, controlFlag, options);
    }
    
}

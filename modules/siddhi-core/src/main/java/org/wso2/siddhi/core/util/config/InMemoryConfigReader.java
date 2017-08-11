/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.util.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Extension config readers
 */
public class InMemoryConfigReader implements ConfigReader {
    private final String keyPrefix;
    private final Map<String, String> configs;

    public InMemoryConfigReader(String keyPrefix, Map<String, String> configs) {
        this.keyPrefix = keyPrefix;
        this.configs = configs;
    }


    @Override
    public String readConfig(String name, String defaultValue) {
        String value = configs.get(keyPrefix + "." + name);
        if (value != null) {
            return value;
        } else {
            return defaultValue;
        }
    }

    @Override
    public Map<String, String> getAllConfigs() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (config.getKey().startsWith(keyPrefix + ".")) {
                map.put(config.getKey().replaceFirst(keyPrefix + ".", ""), config.getValue());
            }
        }
        return map;
    }
}

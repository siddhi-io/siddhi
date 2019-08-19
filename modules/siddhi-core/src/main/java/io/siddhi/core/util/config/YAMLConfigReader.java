/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util.config;

import java.util.Map;

/**
 * Extension config readers
 */
public class YAMLConfigReader implements ConfigReader {
    private final Map<String, String> configs;

    public YAMLConfigReader(Map<String, String> configs) {
        this.configs = configs;
    }

    @Override
    public String readConfig(String name, String defaultValue) {
        String value = configs.get(name);
        if (value != null) {
            return value;
        } else {
            return defaultValue;
        }
    }

    @Override
    public Map<String, String> getAllConfigs() {
        return configs;
    }
}

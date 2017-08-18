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
 * In-memory Siddhi Configuration Manager.
 */
public class InMemoryConfigManager implements ConfigManager {
    private Map<String, String> extensionMasterConfigs = new HashMap<>();
    private Map<String, String> storeConfigs = new HashMap<>();

    public InMemoryConfigManager(Map<String, String> extensionMasterConfigs,
                                 Map<String, String> storeConfigs) {
        if (extensionMasterConfigs != null) {
            this.extensionMasterConfigs = extensionMasterConfigs;
        }
        if (storeConfigs != null) {
            this.storeConfigs = storeConfigs;
        }
    }

    public InMemoryConfigManager() {

    }

    public ConfigReader generateConfigReader(String namespace, String name) {
        String keyPrefix = namespace + "." + name;
        Map<String, String> configs = new HashMap<>();
        for (Map.Entry<String, String> config : extensionMasterConfigs.entrySet()) {
            if (config.getKey().startsWith(keyPrefix)) {
                configs.put(config.getKey(), config.getValue());
            }
        }
        return new InMemoryConfigReader(keyPrefix, configs);
    }

    public Map<String, String> extractStoreConfigs(String name) {
        Map<String, String> configs = new HashMap<>();
        for (Map.Entry<String, String> store : storeConfigs.entrySet()) {
            if (store.getKey().startsWith(name)) {
                configs.put(store.getKey().replaceFirst(name + ".", ""), store.getValue());
            }
        }
        return configs;
    }
}


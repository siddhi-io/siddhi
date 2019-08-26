/*
 * Copyright (c)  2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.util.config.model;

import java.util.HashMap;
import java.util.Map;

/**
 * A second level configuration bean class for siddhi extension config.
 */
public class ExtensionChildConfiguration {

    private String name = "";
    private String namespace = "";
    private Map<String, String> properties = new HashMap<>();

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getNamespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return "name : " + name + ", namespace : " + namespace + ", properties : " + properties.toString();
    }
}


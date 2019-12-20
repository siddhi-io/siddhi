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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code RootConfiguration} is a bean class for root level configuration.
 */
public class RootConfiguration {

    private List<Extension> extensions;
    private List<Reference> refs;
    private Map<String, String> properties;

    public RootConfiguration() {
        this.extensions = new ArrayList<>();
        this.refs = new ArrayList<>();
        this.properties = new HashMap<>();
    }

    public List<Extension> getExtensions() {
        return extensions;
    }

    public List<Reference> getRefs() {
        return refs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

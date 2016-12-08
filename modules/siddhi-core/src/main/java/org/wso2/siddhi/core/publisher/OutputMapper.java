/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.publisher;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.io.map.AttributeMapping;
import org.wso2.siddhi.query.api.execution.io.map.Mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OutputMapper {
    private List<Converter> attributeConverters;
    private Map<String, Converter> dynamicOptionConverters;
    private boolean isCustomMappingEnabled;

    /**
     * This will be called only once and this can be used to acquire
     * required resources for processing the mapping.
     */
    abstract void init(StreamDefinition streamDefinition,
                       Map<String, String> options,
                       List<String> dynamicOptions);

    abstract Object mapDefault(Event event, Map<String, String> dynamicOptions);

    abstract Object mapCustom(Event event, List<String> mappings,
                              Map<String, String> dynamicOptions);

    public final void init(StreamDefinition streamDefinition, Mapping mapping) {
        isCustomMappingEnabled = mapping.getAttributeMappingList().size() > 0;
        attributeConverters = new ArrayList<Converter>();
        if (isCustomMappingEnabled) {
            for (AttributeMapping attributeMapping : mapping.getAttributeMappingList()) {
                attributeConverters.add(new Converter(streamDefinition, attributeMapping.getMapping()));
            }
        }

        List<String> dynamicOptions = new ArrayList<String>();
        dynamicOptionConverters = new HashMap<String, Converter>();
        for (Map.Entry<String, String> entry : mapping.getDynamicOptions().entrySet()) {
            dynamicOptionConverters.put(entry.getKey(),
                    new Converter(streamDefinition, entry.getValue()));
            dynamicOptions.add(entry.getKey());
        }

        init(streamDefinition, mapping.getOptions(), dynamicOptions);
    }

    public final Object mapEvent(Event event) {
        if (isCustomMappingEnabled) {
            return mapCustom(event, getMappedAttributes(event), getMappedOptions(event));
        } else {
            return mapDefault(event, getMappedOptions(event));
        }
    }

    private Map<String, String> getMappedOptions(Event event) {
        return Converter.convert(event, dynamicOptionConverters);
    }

    private List<String> getMappedAttributes(Event event) {
        return Converter.convert(event, attributeConverters);
    }
}

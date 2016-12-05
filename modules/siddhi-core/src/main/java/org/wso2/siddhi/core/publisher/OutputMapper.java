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
import org.wso2.siddhi.query.api.execution.io.map.Mapping;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class OutputMapper {

    private static final Pattern DYNAMIC_PATTERN = Pattern.compile("\\{\\{(.*?)}}");
    private boolean customMappingEnabled = false;
    private Mapping mapping;
    private String mappingString;
    private StreamDefinition streamDefinition;

    /**
     * This will be called only once and this can be used to acquire
     * required resources for processing the mapping.
     */
    abstract void init();

    /**
     * Generate mapping string if the mapping body haven't provided.
     *
     * @param streamDefinition {@link StreamDefinition}
     * @return mapping string
     */
    abstract String generateDefaultMapping(StreamDefinition streamDefinition);

    public final void init(Mapping mapping, StreamDefinition streamDefinition) {
        this.mapping = mapping;
        this.streamDefinition = streamDefinition;
        if (mapping.getBody() != null && !mapping.getBody().isEmpty()) {
            customMappingEnabled = true;
            mappingString = mapping.getBody();
        } else {
            mappingString = generateDefaultMapping(streamDefinition);
        }
        init();
    }

    public final Mapping getMapping() {
        return mapping;
    }

    public final String getMappingFormat() {
        return mapping.getFormat();
    }

    public final String getMappingString() {
        return mappingString;
    }

    public final StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public final boolean isCustomMappingEnabled() {
        return customMappingEnabled;
    }

    /**
     * Map the event according to given mapping.
     *
     * @param event event to be mapped with the given mapping.
     * @return mapped event object
     */
    public final String mapEvent(Event event, String mappingText) {
        Matcher matcher = DYNAMIC_PATTERN.matcher(mappingText);
        while (matcher.find()) {
            if (getValue(event, matcher.group(1)) != null) {
                mappingText = mappingText.replaceAll(
                        String.format("\\{\\{%s}}", matcher.group(1)),
                        getValue(event, matcher.group(1)).toString()
                );
            }
        }
        return mappingText;
    }

    public final Map<String, String> mapDynamicOptions(Event event, Map<String, String> dynamicOptions) {
        Map<String, String> mappedOptions = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : dynamicOptions.entrySet()) {
            mappedOptions.put(entry.getKey(), mapEvent(event, entry.getValue()));
        }
        return mappedOptions;
    }

    public final Object getValue(Event event, String property) {
        List<String> properties = Arrays.asList(streamDefinition.getAttributeNameArray());
        try {
            return event.getData()[properties.indexOf(property)];
        } catch (ArrayIndexOutOfBoundsException e) {
            if (event.getArbitraryDataMap() != null) {
                return event.getArbitraryDataMap().get(property);
            } else {
                return null;
            }
        }
    }

}

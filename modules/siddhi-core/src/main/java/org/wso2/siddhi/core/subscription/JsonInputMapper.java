/*
 * Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.wso2.siddhi.core.subscription;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import org.wso2.siddhi.core.query.output.callback.OutputCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.io.map.AttributeMapping;

import java.util.List;
import java.util.Map;

public class JsonInputMapper implements InputMapper {

    private static final Logger log = Logger.getLogger(JsonInputMapper.class);
    public static final String DEFAULT_JSON_MAPPING_PREFIX = "$.";
    private StreamDefinition outputStreamDefinition;
    private OutputCallback outputCallback;
    private StreamEventPool streamEventPool;
    private StreamEventConverter streamEventConverter;
    private Map<String, String> options;
    private List<AttributeMapping> attributeMappingList;
    private MappingPositionData[] mappingPositionDatas;


    @Override
    public void init(StreamDefinition outputStreamDefinition, OutputCallback outputCallback, MetaStreamEvent metaStreamEvent, Map<String, String> options, List<AttributeMapping> attributeMappingList) {
        this.outputStreamDefinition = outputStreamDefinition;
        this.outputCallback = outputCallback;
        this.outputStreamDefinition = metaStreamEvent.getOutputStreamDefinition();
        this.options = options;
        this.attributeMappingList = attributeMappingList;
        this.streamEventConverter = new ZeroStreamEventConverter();
        this.streamEventPool = new StreamEventPool(metaStreamEvent, 5);

        final int attributesSize = this.outputStreamDefinition.getAttributeList().size();
        this.mappingPositionDatas = new MappingPositionData[attributesSize];

        // Create the position mapping arrays
        if (this.attributeMappingList != null && this.attributeMappingList.size() > 0) {
            // Custom mapping parameters are given
            for (int i = 0; i < attributeMappingList.size(); i++) {
                AttributeMapping attributeMapping = attributeMappingList.get(i);
                String attributeName = attributeMapping.getRename();
                int position;

                if (attributeName != null) {
                    // Use the name to determine the position
                    position = outputStreamDefinition.getAttributePosition(attributeName);
                } else {
                    // Use the same order as provided by the user
                    position = i;
                }
                this.mappingPositionDatas[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        } else {
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositionDatas[i] = new MappingPositionData(i, DEFAULT_JSON_MAPPING_PREFIX + this.outputStreamDefinition.getAttributeList().get(i).getName());
            }
        }

    }

    @Override
    public void onEvent(Object eventObject) {

        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertEvent(convertToEvent(eventObject), borrowedEvent);

        outputCallback.send(new ComplexEventChunk<StreamEvent>(borrowedEvent, borrowedEvent, true));
    }

    private Event convertToEvent(Object eventObject) {
        // Validate the event
        if (eventObject == null) {
            log.error("Null object received from the InputTransport");
        }
        if (!(eventObject instanceof String)) {
            log.error("Invalid JSON object received. Expected String, found " + eventObject.getClass().getCanonicalName());
        }

        Event event = new Event(this.outputStreamDefinition.getAttributeList().size());
        Object[] data = event.getData();
        List<Attribute> streamAttributes = this.outputStreamDefinition.getAttributeList();

        // Parse the JSON string
        ReadContext readContext = JsonPath.parse(eventObject.toString());

        for (MappingPositionData mappingPositionData : this.mappingPositionDatas) {
            int position = mappingPositionData.getPosition();
            Attribute attribute = streamAttributes.get(position);
            data[position] = AttributeConverter.getPropertyValue(readContext.read(mappingPositionData.getMapping()), attribute.getType());
        }

        return event;
    }

    private class MappingPositionData {
        private int position;
        private String mapping;

        public MappingPositionData(int position, String mapping) {
            this.position = position;
            this.mapping = mapping;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}

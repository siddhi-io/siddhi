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
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.execution.io.map.AttributeMapping;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextInputMapper implements InputMapper {

    private static final Logger log = Logger.getLogger(TextInputMapper.class);
    public static final String DEFAULT_MAPPING_REGEX = "[^,;]+";
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

        // Create the position regex arrays
        if (this.attributeMappingList != null && this.attributeMappingList.size() > 0) {
            // Custom regex parameters are given
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
                String[] mappingComponents = attributeMapping.getMapping().split("\\[");
                String regex = this.options.get(mappingComponents[0]);
                int index = Integer.parseInt(mappingComponents[1].substring(0, mappingComponents[1].length() - 1));
                if (regex == null) {
                    throw new ExecutionPlanValidationException("The regex " + mappingComponents[0] + " does not have a regex definition");
                }
                this.mappingPositionDatas[i] = new MappingPositionData(position, regex, index);
            }
        } else {
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositionDatas[i] = new MappingPositionData(i, DEFAULT_MAPPING_REGEX, i);
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

        for (MappingPositionData mappingPositionData : this.mappingPositionDatas) {
            int position = mappingPositionData.getPosition();
            Attribute attribute = streamAttributes.get(position);
            data[position] = AttributeConverter.getPropertyValue(mappingPositionData.match(eventObject.toString()), attribute.getType());
        }

        return event;
    }


    private class MappingPositionData {
        private int position;
        private String regex;
        private int groupIndex;

        public MappingPositionData(int position, String regex, int groupIndex) {
            this.position = position;
            this.regex = regex;
            this.groupIndex = groupIndex;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getRegex() {
            return regex;
        }

        public void setRegex(String regex) {
            this.regex = regex;
        }

        public int getGroupIndex() {
            return groupIndex;
        }

        public void setGroupIndex(int groupIndex) {
            this.groupIndex = groupIndex;
        }

        public String match(String text) {
            String matchedText;
            Pattern pattern = Pattern.compile(this.regex);
            Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                matchedText = matcher.group(this.groupIndex);
            } else {
                matchedText = null;
            }

            return matchedText;
        }
    }
}

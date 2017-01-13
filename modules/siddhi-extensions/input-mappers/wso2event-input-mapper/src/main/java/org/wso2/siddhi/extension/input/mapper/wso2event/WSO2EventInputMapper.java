/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.input.mapper.wso2event;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.query.output.callback.OutputCallback;
import org.wso2.siddhi.core.subscription.InputMapper;
import org.wso2.siddhi.core.subscription.InputTransport;
import org.wso2.siddhi.core.util.AttributeConverter;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.io.map.AttributeMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
// TODO: 1/9/17  
/**
 * This mapper converts hashmap input to {@link ComplexEventChunk}.
 */
public class WSO2EventInputMapper implements InputMapper {

    /**
     * Logger to log the events.
     */
    private static final Logger log = Logger.getLogger(WSO2EventInputMapper.class);

    /**
     * OutputCallback to which the converted event must be sent.
     */
    private OutputCallback outputCallback;

    /**
     * Output StreamDefinition of the input mapper.
     */
    private StreamDefinition outputStreamDefinition;

    /**
     * StreamEventPool used to borrow a new event.
     */
    private StreamEventPool streamEventPool;

    /**
     * StreamEventConverter to convert {@link Event} to {@link StreamEvent}.
     */
    private StreamEventConverter streamEventConverter;

    /**
     * Array of information about event mapping.
     */
    private MappingPositionData[] mappingPositions;

    /**
     * Attributes of the output stream.
     */
    private List<Attribute> streamAttributes;

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param outputStreamDefinition the output StreamDefinition
     * @param outputCallback         the OutputCallback to which the output has to be sent
     * @param metaStreamEvent        the MetaStreamEvent
     * @param options                additional mapping options
     * @param attributeMappingList   list of attributes mapping
     */
    @Override
    public void init(StreamDefinition outputStreamDefinition, OutputCallback outputCallback, MetaStreamEvent metaStreamEvent, Map<String, String> options, List<AttributeMapping> attributeMappingList) {
        this.outputStreamDefinition = outputStreamDefinition;
        this.outputCallback = outputCallback;
        this.outputStreamDefinition = metaStreamEvent.getOutputStreamDefinition();
        this.streamEventConverter = new ZeroStreamEventConverter();
        this.streamEventPool = new StreamEventPool(metaStreamEvent, 5);
        this.streamAttributes = this.outputStreamDefinition.getAttributeList();

        int attributesSize = this.outputStreamDefinition.getAttributeList().size();
        this.mappingPositions = new MappingPositionData[attributesSize];

        // Create the position mapping arrays
        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            // Custom mapping parameters are given
            for (int i = 0; i < attributeMappingList.size(); i++) {
                // i represents the position of attributes as given by the user in mapping

                AttributeMapping attributeMapping = attributeMappingList.get(i);

                // The optional name given by the user in attribute mapping
                String attributeName = attributeMapping.getRename();

                // The position of the attribute in the output stream definition
                int position;

                if (attributeName != null) {
                    // Use the name to determine the position
                    position = outputStreamDefinition.getAttributePosition(attributeName);
                } else {
                    // Use the same order as provided by the user
                    position = i;
                }
                this.mappingPositions[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        } else {
            // Use the attribute names of the output stream in order
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositions[i] = new MappingPositionData(i, this
                        .outputStreamDefinition.getAttributeList().get(i).getName());
            }
        }
    }

    /**
     * Receive {@link Event} or Object[] from {@link InputTransport}, convert to {@link ComplexEventChunk} and send
     * to the {@link OutputCallback}.
     *
     * @param eventObject the hashmap
     */
    @Override
    public void onEvent(Object eventObject) {
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertEvent(convertToEvent(eventObject), borrowedEvent);
        outputCallback.send(new ComplexEventChunk<StreamEvent>(borrowedEvent, borrowedEvent, true));
    }

    private Event convertToEvent(Object eventObject) {
        if (eventObject == null) {
            throw new ExecutionPlanRuntimeException("Null object received from the InputTransport to MapInputMapper");
        } else if (!(eventObject instanceof HashMap)) {
            throw new ExecutionPlanRuntimeException("Invalid Map object received. Expected HashMap, but found " +
                    eventObject.getClass()
                            .getCanonicalName());
        }

        Event event = new Event(this.outputStreamDefinition.getAttributeList().size());
        Object[] data = event.getData();

        for (MappingPositionData mappingPositionData : this.mappingPositions) {
            int position = mappingPositionData.getPosition();
            data[position] = AttributeConverter.getPropertyValue(((HashMap) eventObject).get(mappingPositionData.getMapping()),
                    streamAttributes.get(position).getType());
        }

        return event;
    }

    /**
     * A POJO class which holds the attribute position in output stream and the user defined mapping.
     */
    private class MappingPositionData {
        /**
         * Attribute position in the output stream.
         */
        private int position;

        /**
         * The hashmap mapping as defined by the user.
         */
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

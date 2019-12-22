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
package io.siddhi.core.event.stream;

import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;


/**
 * Class to hold meta info about StreamEvent.
 * This is also used to update variable positions on executors
 */
public class MetaStreamEvent implements MetaComplexEvent {
    private List<Attribute> beforeWindowData = new ArrayList<Attribute>();
    private List<Attribute> onAfterWindowData = null;
    private List<Attribute> outputData = null;
    private List<AbstractDefinition> inputDefinitions = new ArrayList<AbstractDefinition>();
    private String inputReferenceId;
    private StreamDefinition outputStreamDefinition;
    private EventType eventType = EventType.DEFAULT;
    //Event has the possibility to produce multiple values
    private boolean multiValue = false;

    public List<Attribute> getBeforeWindowData() {
        return beforeWindowData;
    }

    public List<Attribute> getOnAfterWindowData() {
        if (onAfterWindowData != null) {
            return onAfterWindowData;
        } else {
            return new ArrayList<Attribute>();  //return empty arraylist to avoid NPE
        }
    }

    public List<Attribute> getOutputData() {
        if (outputData != null) {
            return outputData;
        } else {
            return new ArrayList<Attribute>();  //return empty arraylist to avoid NPE
        }
    }

    public void initializeOnAfterWindowData() {
        if (onAfterWindowData == null) {
            onAfterWindowData = new ArrayList<Attribute>();
        }
    }

    @Deprecated
    public void initializeAfterWindowData() {
        if (onAfterWindowData == null) {
            onAfterWindowData = new ArrayList<Attribute>();
        }
    }

    /**
     * Universal method to add data to MetaStream event.
     * Will make sure event will be added to corresponding array by
     * initializing them accordingly.
     *
     * @param attribute attribute
     * @return attribute position
     */
    public int addData(Attribute attribute) {
        if (onAfterWindowData != null) {
            if (!onAfterWindowData.contains(attribute)) {
                onAfterWindowData.add(attribute);
                return SiddhiConstants.ON_AFTER_WINDOW_DATA_INDEX;
            }
        } else {
            if (!beforeWindowData.contains(attribute)) {
                beforeWindowData.add(attribute);
                return SiddhiConstants.BEFORE_WINDOW_DATA_INDEX;
            }
        }
        return SiddhiConstants.UNKNOWN_STATE;
    }

    public void addOutputData(Attribute attribute) {
        if (outputData == null) {
            outputData = new ArrayList<Attribute>();
        }
        if (!outputData.contains(attribute)) {
            outputData.add(attribute);
        }
    }

    public void addOutputDataAllowingDuplicate(Attribute attribute) {
        if (outputData == null) {
            outputData = new ArrayList<Attribute>();
        }
        outputData.add(attribute);
    }

    public List<AbstractDefinition> getInputDefinitions() {
        return inputDefinitions;
    }

    public void addInputDefinition(AbstractDefinition inputDefinition) {
        this.inputDefinitions.add(inputDefinition);
    }

    public String getInputReferenceId() {
        return inputReferenceId;
    }

    public void setInputReferenceId(String inputReferenceId) {
        this.inputReferenceId = inputReferenceId;
    }

    @Override
    public void setOutputDefinition(StreamDefinition streamDefinition) {
        outputStreamDefinition = streamDefinition;
    }

    @Override
    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public AbstractDefinition getLastInputDefinition() {
        return inputDefinitions.get(inputDefinitions.size() - 1);
    }

    public boolean isMultiValue() {
        return multiValue;
    }

    public void setMultiValue(boolean multiValue) {
        this.multiValue = multiValue;
    }

    public MetaStreamEvent clone() {
        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        if (outputData != null) {
            for (Attribute attribute : outputData) {
                metaStreamEvent.addOutputData(attribute);
            }
        }
        if (this.onAfterWindowData != null) {
            metaStreamEvent.onAfterWindowData = new ArrayList<>(onAfterWindowData);
        }
        metaStreamEvent.beforeWindowData = new ArrayList<>(beforeWindowData);

        for (AbstractDefinition abstractDefinition : this.getInputDefinitions()) {
            metaStreamEvent.addInputDefinition(abstractDefinition);
        }
        metaStreamEvent.setInputReferenceId(this.getInputReferenceId());
        metaStreamEvent.setOutputDefinition(this.getOutputStreamDefinition());
        metaStreamEvent.setEventType(this.getEventType());
        metaStreamEvent.setMultiValue(this.isMultiValue());
        return metaStreamEvent;
    }

    /**
     * Type of Meta Events
     */
    public enum EventType {
        TABLE, WINDOW, AGGREGATE, DEFAULT
    }
}

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
package io.siddhi.core.event.state;

import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to hold mapping between StreamEvents and respective StateEvent.
 * This consist of array of MetaStreamEvents which represent each
 * StreamEvent within StateEvent
 */
public class MetaStateEvent implements MetaComplexEvent {

    private MetaStreamEvent[] metaStreamEvents;
    private int streamEventCount = 0;
    private StreamDefinition outputStreamDefinition;

    //    private List<MetaStateEventAttribute> preOutputDataAttributes = new ArrayList<MetaStateEventAttribute>();
    private List<MetaStateEventAttribute> outputDataAttributes;

    public MetaStateEvent(int size) {
        metaStreamEvents = new MetaStreamEvent[size];
    }

    public MetaStateEvent(MetaStreamEvent[] metaStreamEvents) {
        this.metaStreamEvents = metaStreamEvents.clone();
        streamEventCount = metaStreamEvents.length;
    }

    public MetaStreamEvent getMetaStreamEvent(int position) {
        return metaStreamEvents[position];
    }

    public void addEvent(MetaStreamEvent metaStreamEvent) {
        metaStreamEvents[streamEventCount] = metaStreamEvent;
        streamEventCount++;
    }

    public void addOutputDataAllowingDuplicate(MetaStateEventAttribute metaStateEventAttribute) {
        if (outputDataAttributes == null) {
            outputDataAttributes = new ArrayList<MetaStateEventAttribute>();
        }
        outputDataAttributes.add(metaStateEventAttribute);
    }

    public List<MetaStateEventAttribute> getOutputDataAttributes() {
        if (outputDataAttributes != null) {
            return outputDataAttributes;
        } else {
            return new ArrayList<MetaStateEventAttribute>();  //return empty arraylist to avoid NPE
        }
    }

    public int getStreamEventCount() {
        return streamEventCount;
    }

    public MetaStreamEvent[] getMetaStreamEvents() {
        return metaStreamEvents;
    }

    public void setOutputDefinition(StreamDefinition streamDefinition) {
        this.outputStreamDefinition = streamDefinition;
    }

    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    public MetaStateEvent clone() {
        MetaStateEvent metaStateEvent = new MetaStateEvent(this.getStreamEventCount());
        for (MetaStreamEvent metaStreamEvent : this.getMetaStreamEvents()) {
            metaStateEvent.addEvent(metaStreamEvent.clone());
        }
        if (this.getOutputStreamDefinition() != null) {
            metaStateEvent.setOutputDefinition(this.getOutputStreamDefinition().clone());
        }
        return metaStateEvent;
    }
}

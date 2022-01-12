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
package io.siddhi.core.stream.output;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StreamCallback is used to receive events from {@link StreamJunction}. This class should be extended if one intends
 * to get events from a Siddhi Stream.
 */
public abstract class StreamCallback implements StreamJunction.Receiver {

    private static final Logger log = LogManager.getLogger(StreamCallback.class);

    private String streamId;
    private AbstractDefinition streamDefinition;
    private SiddhiAppContext siddhiAppContext;

    @Override
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public AbstractDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public void setStreamDefinition(AbstractDefinition streamDefinition) {
        this.streamDefinition = streamDefinition;
    }

    public void setContext(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
    }

    public Map<String, Object> toMap(Event event) {
        if (event != null) {
            Map<String, Object> mapEvent = new HashMap<>();
            Object[] data = event.getData();
            List<Attribute> attributeList = streamDefinition.getAttributeList();
            mapEvent.put("_timestamp", event.getTimestamp());
            for (int i = 0; i < data.length; i++) {
                mapEvent.put(attributeList.get(i).getName(), data[i]);
            }
            return mapEvent;
        }
        return null;
    }

    public Map<String, Object>[] toMap(Event[] events) {
        if (events != null) {
            Map[] mapEvents = new Map[events.length];
            for (int i = 0; i < events.length; i++) {
                mapEvents[i] = toMap(events[i]);
            }
            return mapEvents;
        }
        return null;
    }

    @Override
    public void receive(ComplexEvent complexEvent) {
        List<Event> eventBuffer = new ArrayList<Event>();
        while (complexEvent != null) {
            eventBuffer.add(new Event(complexEvent.getOutputData().length).copyFrom(complexEvent));
            complexEvent = complexEvent.getNext();
        }
        if (eventBuffer.size() == 1) {
            receive(eventBuffer.get(0));
        } else {
            receiveEvents(eventBuffer.toArray(new Event[eventBuffer.size()]));
        }
    }

    @Override
    public void receive(Event event) {
        receiveEvents(new Event[]{event});
    }

    @Override
    public void receive(List<Event> events) {
        receiveEvents(events.toArray(new Event[events.size()]));
    }

    public void receive(long timestamp, Object[] data) {
        receiveEvents(new Event[]{new Event(timestamp, data)});
    }

    public void receiveEvents(Event[] events) {
        try {
            receive(events);
        } catch (RuntimeException e) {
            log.error("Error on sending events " + Arrays.deepToString(events) +
                    " in the SiddhiApp '" + siddhiAppContext.getName() + "'", e);
        }
    }

    public abstract void receive(Event[] events);

    public void startProcessing() {

    }

    public void stopProcessing() {

    }

}

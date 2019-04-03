/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.event.stream.converter;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;

import java.util.List;

/**
 * A StreamEvent holder that can also convert other events into StreamEvents
 */
public class FaultStreamEventConverter {

    private StreamEventFactory streamEventFactory;

    public FaultStreamEventConverter(StreamEventFactory streamEventFactory) {
        this.streamEventFactory = streamEventFactory;
    }


    public StreamEvent convert(Event event, Exception e) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        convertEvent(event, newEvent, e);
        return newEvent;
    }

    public StreamEvent convert(long timestamp, Object[] data, Exception e) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        convertData(timestamp, data, newEvent, e);
        return newEvent;
    }

    public StreamEvent convert(ComplexEvent complexEvents, Exception e) {
        StreamEvent firstEvent = streamEventFactory.newInstance();
        convertComplexEvent(complexEvents, firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        complexEvents = complexEvents.getNext();
        while (complexEvents != null) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            convertComplexEvent(complexEvents, nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
            complexEvents = complexEvents.getNext();
        }
        return firstEvent;
    }

    public StreamEvent convert(Event[] events, Exception e) {
        StreamEvent firstEvent = streamEventFactory.newInstance();
        convertEvent(events[0], firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.length; i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            convertEvent(events[i], nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        return firstEvent;
    }

    public StreamEvent convert(List<Event> events, Exception e) {
        StreamEvent firstEvent = streamEventFactory.newInstance();
        convertEvent(events.get(0), firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.size(); i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            convertEvent(events.get(i), nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        return firstEvent;
    }

    private void convertData(long timestamp, Object[] data, StreamEvent.Type type, StreamEvent newEvent,
                             Exception e) {
        System.arraycopy(data, 0, newEvent.getOutputData(), 0, data.length);
        newEvent.setOutputData(e, data.length);
        newEvent.setType(type);
        newEvent.setTimestamp(timestamp);
    }


    private void convertEvent(Event event, StreamEvent newEvent, Exception e) {
        convertData(event.getTimestamp(), event.getData(),
                event.isExpired() ? StreamEvent.Type.EXPIRED : StreamEvent.Type.CURRENT,
                newEvent, e);
    }

    private void convertComplexEvent(ComplexEvent complexEvent, StreamEvent newEvent, Exception e) {
        convertData(complexEvent.getTimestamp(), complexEvent.getOutputData(), complexEvent.getType(),
                newEvent, e);
    }

    private void convertData(long timeStamp, Object[] data, StreamEvent newEvent, Exception e) {
        convertData(timeStamp, data, StreamEvent.Type.CURRENT, newEvent, e);
    }

}

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

package org.wso2.siddhi.core.event.stream.converter;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;

import java.util.List;

/**
 * A StreamEvent holder that can also convert other events into StreamEvents
 */
public class FaultStreamEventConverter {

    private StreamEventPool streamEventPool;

    public FaultStreamEventConverter(StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
    }


    public StreamEvent convert(Event event, Exception e) {
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        convertEvent(event, borrowedEvent, e);
        return borrowedEvent;
    }

    public StreamEvent convert(long timestamp, Object[] data, Exception e) {
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        convertData(timestamp, data, borrowedEvent, e);
        return borrowedEvent;
    }

    public StreamEvent convert(ComplexEvent complexEvents, Exception e) {
        StreamEvent firstEvent = streamEventPool.borrowEvent();
        convertComplexEvent(complexEvents, firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        complexEvents = complexEvents.getNext();
        while (complexEvents != null) {
            StreamEvent nextEvent = streamEventPool.borrowEvent();
            convertComplexEvent(complexEvents, nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
            complexEvents = complexEvents.getNext();
        }
        return firstEvent;
    }

    public StreamEvent convert(Event[] events, Exception e) {
        StreamEvent firstEvent = streamEventPool.borrowEvent();
        convertEvent(events[0], firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.length; i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventPool.borrowEvent();
            convertEvent(events[i], nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        return firstEvent;
    }

    public StreamEvent convert(List<Event> events, Exception e) {
        StreamEvent firstEvent = streamEventPool.borrowEvent();
        convertEvent(events.get(0), firstEvent, e);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.size(); i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventPool.borrowEvent();
            convertEvent(events.get(i), nextEvent, e);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        return firstEvent;
    }

    private void convertData(long timestamp, Object[] data, StreamEvent.Type type, StreamEvent borrowedEvent,
                             Exception e) {
        System.arraycopy(data, 0, borrowedEvent.getOutputData(), 0, data.length);
        borrowedEvent.setOutputData(e, data.length);
        borrowedEvent.setType(type);
        borrowedEvent.setTimestamp(timestamp);
    }


    private void convertEvent(Event event, StreamEvent borrowedEvent, Exception e) {
        convertData(event.getTimestamp(), event.getData(),
                event.isExpired() ? StreamEvent.Type.EXPIRED : StreamEvent.Type.CURRENT,
                borrowedEvent, e);
    }

    private void convertComplexEvent(ComplexEvent complexEvent, StreamEvent borrowedEvent, Exception e) {
        convertData(complexEvent.getTimestamp(), complexEvent.getOutputData(), complexEvent.getType(),
                borrowedEvent, e);
    }

    private void convertData(long timeStamp, Object[] data, StreamEvent borrowedEvent, Exception e) {
        convertData(timeStamp, data, StreamEvent.Type.CURRENT, borrowedEvent, e);
    }

}

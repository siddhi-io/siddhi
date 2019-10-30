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

package io.siddhi.core.event.stream.converter;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;

/**
 * A StreamEvent holder that can also convert other events into StreamEvents
 */
public class ConversionStreamEventChunk extends ComplexEventChunk<StreamEvent> {

    private static final long serialVersionUID = 2754352338846132676L;
    private StreamEventConverter streamEventConverter;
    private StreamEventFactory streamEventFactory;

    public ConversionStreamEventChunk(MetaStreamEvent metaStreamEvent, StreamEventFactory streamEventFactory) {
        this.streamEventFactory = streamEventFactory;
        streamEventConverter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
    }

    public ConversionStreamEventChunk(StreamEventConverter streamEventConverter,
                                      StreamEventFactory streamEventFactory) {
        this.streamEventConverter = streamEventConverter;
        this.streamEventFactory = streamEventFactory;
    }

    public void convertAndAssign(Event event) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertEvent(event, newEvent);
        first = newEvent;
        last = first;
    }

    public void convertAndAssign(long timestamp, Object[] data) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertData(timestamp, data, newEvent);
        first = newEvent;
        last = first;
    }

    public void convertAndAssign(ComplexEvent complexEvent) {
        first = streamEventFactory.newInstance();
        last = convertAllStreamEvents(complexEvent, first);
    }

//    @Override
//    public void convertAndAssignFirst(StreamEvent streamEvent) {
//        StreamEvent newEvent = streamEventFactory.borrowEvent();
//        eventConverter.convertComplexEvent(streamEvent, newEvent);
//        first = newEvent;
//        last = first;
//    }

    public void convertAndAssign(Event[] events) {
        StreamEvent firstEvent = streamEventFactory.newInstance();
        streamEventConverter.convertEvent(events[0], firstEvent);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.length; i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            streamEventConverter.convertEvent(events[i], nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        first = firstEvent;
        last = currentEvent;
    }

    public void convertAndAdd(Event event) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertEvent(event, newEvent);

        if (first == null) {
            first = newEvent;
            last = first;
        } else {
            last.setNext(newEvent);
            last = newEvent;
        }

    }

    private StreamEvent convertAllStreamEvents(ComplexEvent complexEvents, StreamEvent firstEvent) {
        streamEventConverter.convertComplexEvent(complexEvents, firstEvent);
        StreamEvent currentEvent = firstEvent;
        complexEvents = complexEvents.getNext();
        while (complexEvents != null) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            streamEventConverter.convertComplexEvent(complexEvents, nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
            complexEvents = complexEvents.getNext();
        }
        return currentEvent;
    }
}

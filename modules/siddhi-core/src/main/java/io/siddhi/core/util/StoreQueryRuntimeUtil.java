/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.query.selector.QuerySelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to hold util functions used during StoreQueryRuntime
 */
public class StoreQueryRuntimeUtil {
    public static Event[] executeSelector(StreamEvent streamEvents, QuerySelector selector,
                                          StateEventFactory stateEventFactory,
                                          MetaStreamEvent.EventType eventType) {
        ComplexEventChunk outputComplexEventChunk = executeSelectorAndReturnChunk(streamEvents, selector,
                stateEventFactory, eventType);
        if (outputComplexEventChunk != null) {
            List<Event> events = new ArrayList<>();
            outputComplexEventChunk.reset();
            while (outputComplexEventChunk.hasNext()) {
                ComplexEvent complexEvent = outputComplexEventChunk.next();
                events.add(new Event(complexEvent.getTimestamp(), complexEvent.getOutputData()));
            }
            return events.toArray(new Event[0]);
        } else {
            return null;
        }

    }

    private static ComplexEventChunk executeSelectorAndReturnChunk(StreamEvent streamEvents, QuerySelector selector,
                                                                   StateEventFactory stateEventFactory,
                                                                   MetaStreamEvent.EventType eventType) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>(true);
        while (streamEvents != null) {

            StreamEvent streamEvent = streamEvents;
            streamEvents = streamEvents.getNext();
            streamEvent.setNext(null);

            StateEvent stateEvent = stateEventFactory.newInstance();
            if (eventType == MetaStreamEvent.EventType.AGGREGATE) {
                stateEvent.addEvent(1, streamEvent);
            } else {
                stateEvent.addEvent(0, streamEvent);
            }
            complexEventChunk.add(stateEvent);
        }
        return selector.execute(complexEventChunk);
    }

    public static StreamEvent executeSelectorAndReturnStreamEvent(StreamEvent streamEvents, QuerySelector selector,
                                                                  StateEventFactory stateEventFactory,
                                                                  MetaStreamEvent.EventType eventType) {
        ComplexEventChunk outputComplexEventChunk = executeSelectorAndReturnChunk(streamEvents, selector,
                stateEventFactory, eventType);
        if (outputComplexEventChunk != null) {
            outputComplexEventChunk.reset();
            ComplexEvent firstComplexEvent = outputComplexEventChunk.next();
            StreamEvent firstEvent = new StreamEvent(0, 0, firstComplexEvent.getOutputData().length);
            firstEvent.setOutputData(firstComplexEvent.getOutputData());
            StreamEvent eventPointer = firstEvent;
            while (outputComplexEventChunk.hasNext()) {
                ComplexEvent complexEvent = outputComplexEventChunk.next();
                StreamEvent streamEvent = new StreamEvent(0, 0, complexEvent.getOutputData().length);
                streamEvent.setOutputData(complexEvent.getOutputData());
                eventPointer.setNext(streamEvent);
                eventPointer = streamEvent;
            }
            return firstEvent;
        } else {
            return null;
        }
    }
}

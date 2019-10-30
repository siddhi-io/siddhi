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
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.query.selector.QuerySelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to hold util functions used during OnDemandQueryRuntime
 */
public class OnDemandQueryRuntimeUtil {
    public static Event[] executeSelector(StateEventFactory stateEventFactory,
                                          StreamEvent streamEvent, StreamEvent storeEvents,
                                          int storeEventIndex, QuerySelector selector) {
        ComplexEventChunk outputComplexEventChunk = executeSelectorAndReturnChunk(stateEventFactory, streamEvent,
                storeEvents, storeEventIndex, selector);
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

    private static ComplexEventChunk executeSelectorAndReturnChunk(StateEventFactory stateEventFactory,
                                                                   StreamEvent streamEvent, StreamEvent storeEvents,
                                                                   int storeEventIndex, QuerySelector selector) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>();
        while (storeEvents != null) {

            StreamEvent storeEvent = storeEvents;
            storeEvents = storeEvents.getNext();
            storeEvent.setNext(null);

            StateEvent joinEvent = stateEventFactory.newInstance();
            if (streamEvent == null) {
                joinEvent.addEvent(storeEventIndex, storeEvent);
            } else {
                joinEvent.addEvent(0, streamEvent);
                joinEvent.addEvent(1, storeEvent);
            }
            complexEventChunk.add(joinEvent);
        }
        return selector.execute(complexEventChunk);
    }

    public static StreamEvent executeSelectorAndReturnStreamEvent(StateEventFactory stateEventFactory,
                                                                  StreamEvent streamEvent, StreamEvent storeEvents,
                                                                  int storeEventIndex, QuerySelector selector) {
        ComplexEventChunk outputComplexEventChunk = executeSelectorAndReturnChunk(stateEventFactory, streamEvent,
                storeEvents, storeEventIndex, selector);
        if (outputComplexEventChunk != null) {
            outputComplexEventChunk.reset();
            ComplexEvent firstComplexEvent = outputComplexEventChunk.next();
            StreamEvent firstEvent = new StreamEvent(0, 0, firstComplexEvent.getOutputData().length);
            firstEvent.setOutputData(firstComplexEvent.getOutputData());
            StreamEvent eventPointer = firstEvent;
            while (outputComplexEventChunk.hasNext()) {
                ComplexEvent complexEvent = outputComplexEventChunk.next();
                StreamEvent resultEvent = new StreamEvent(0, 0, complexEvent.getOutputData().length);
                resultEvent.setOutputData(complexEvent.getOutputData());
                eventPointer.setNext(resultEvent);
                eventPointer = resultEvent;
            }
            return firstEvent;
        } else {
            return null;
        }
    }
}

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
package io.siddhi.core.query;

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.OnDemandQueryRuntimeException;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.window.Window;

import java.util.ArrayList;
import java.util.List;

import static io.siddhi.core.util.OnDemandQueryRuntimeUtil.executeSelector;

/**
 * on-demand query Runtime holds the runtime information needed for executing the on-demand query.
 */
public class FindOnDemandQueryRuntime extends OnDemandQueryRuntime {

    private CompiledCondition compiledCondition;
    private SiddhiQueryContext siddhiQueryContext;
    private Table table;
    private Window window;
    private MetaStreamEvent.EventType eventType;
    private AggregationRuntime aggregation;

    public FindOnDemandQueryRuntime(Table table, CompiledCondition compiledCondition, String queryName,
                                    MetaStreamEvent metaStreamEvent) {
        this.table = table;
        this.compiledCondition = compiledCondition;
        this.queryName = queryName;
        this.eventType = metaStreamEvent.getEventType();
        this.metaStreamEvent = metaStreamEvent;
        this.setOutputAttributes(metaStreamEvent.getLastInputDefinition().getAttributeList());
    }

    public FindOnDemandQueryRuntime(Window window, CompiledCondition compiledCondition, String queryName,
                                    MetaStreamEvent metaStreamEvent) {
        this.window = window;
        this.compiledCondition = compiledCondition;
        this.queryName = queryName;
        this.eventType = metaStreamEvent.getEventType();
        this.metaStreamEvent = metaStreamEvent;
        this.setOutputAttributes(metaStreamEvent.getLastInputDefinition().getAttributeList());
    }

    public FindOnDemandQueryRuntime(AggregationRuntime aggregation, CompiledCondition compiledCondition,
                                    String queryName, MetaStreamEvent metaStreamEvent,
                                    SiddhiQueryContext siddhiQueryContext) {
        this.aggregation = aggregation;
        this.compiledCondition = compiledCondition;
        this.siddhiQueryContext = siddhiQueryContext;
        this.queryName = queryName;
        this.eventType = metaStreamEvent.getEventType();
        this.metaStreamEvent = metaStreamEvent;
        this.setOutputAttributes(metaStreamEvent.getLastInputDefinition().getAttributeList());
    }

    @Override
    public Event[] execute() {
        try {
            StateEvent stateEvent = new StateEvent(1, 0);
            StreamEvent streamEvents = null;
            switch (eventType) {
                case TABLE:
                    streamEvents = table.find(stateEvent, compiledCondition);
                    break;
                case WINDOW:
                    streamEvents = window.find(stateEvent, compiledCondition);
                    break;
                case AGGREGATE:
                    stateEvent = new StateEvent(2, 0);
                    streamEvents = aggregation.find(stateEvent, compiledCondition, siddhiQueryContext);
                    break;
                case DEFAULT:
                    break;
            }
            if (streamEvents == null) {
                return null;
            } else {
                if (selector != null) {
                    if (eventType == MetaStreamEvent.EventType.AGGREGATE) {
                        return executeSelector(stateEventFactory, null, streamEvents, 1, selector);
                    } else {
                        return executeSelector(stateEventFactory, null, streamEvents, 0, selector);
                    }
                } else {
                    List<Event> events = new ArrayList<Event>();
                    while (streamEvents != null) {
                        events.add(new Event(streamEvents.getTimestamp(), streamEvents.getOutputData()));
                        streamEvents = streamEvents.getNext();
                    }
                    return events.toArray(new Event[0]);
                }
            }
        } catch (Throwable t) {
            throw new OnDemandQueryRuntimeException("Error executing '" + queryName + "', " + t.getMessage(), t);
        }
    }

    @Override
    public void reset() {
        if (selector != null) {
            selector.process(generateResetComplexEventChunk(metaStreamEvent));
        }
    }

    @Override
    public TYPE getType() {
        return TYPE.FIND;
    }

    private ComplexEventChunk<ComplexEvent> generateResetComplexEventChunk(MetaStreamEvent metaStreamEvent) {
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setType(ComplexEvent.Type.RESET);

        StateEvent stateEvent = stateEventFactory.newInstance();
        if (eventType == MetaStreamEvent.EventType.AGGREGATE) {
            stateEvent.addEvent(1, streamEvent);
        } else {
            stateEvent.addEvent(0, streamEvent);
        }
        stateEvent.setType(ComplexEvent.Type.RESET);

        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>();
        complexEventChunk.add(stateEvent);
        return complexEventChunk;
    }
}

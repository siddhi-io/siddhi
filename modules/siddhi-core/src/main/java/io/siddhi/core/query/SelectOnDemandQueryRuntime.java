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

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.OnDemandQueryRuntimeException;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * On-demand Query Runtime holds the runtime information needed for executing the on-demand Query.
 */
public class SelectOnDemandQueryRuntime extends OnDemandQueryRuntime {

    private final CompiledSelection compiledSelection;
    private final CompiledCondition compiledCondition;
    private QueryableProcessor queryableProcessor;

    public SelectOnDemandQueryRuntime(QueryableProcessor queryableProcessor, CompiledCondition compiledCondition,
                                      CompiledSelection compiledSelection, List<Attribute> expectedOutputAttributeList,
                                      String queryName) {
        this.queryableProcessor = queryableProcessor;

        this.compiledCondition = compiledCondition;
        this.compiledSelection = compiledSelection;
        this.queryName = queryName;
        this.outputAttributes = expectedOutputAttributeList.toArray(new Attribute[expectedOutputAttributeList.size()]);
    }

    public Event[] execute() {
        try {
            StateEvent stateEvent = new StateEvent(1, 0);
            StreamEvent streamEvents = queryableProcessor.query(stateEvent, compiledCondition,
                    compiledSelection, outputAttributes);
            if (streamEvents == null) {
                return null;
            } else {
                List<Event> events = new ArrayList<Event>();
                while (streamEvents != null) {
                    events.add(new Event(streamEvents.getTimestamp(), streamEvents.getOutputData()));
                    streamEvents = streamEvents.getNext();
                }
                return events.toArray(new Event[0]);
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

    private ComplexEventChunk<ComplexEvent> generateResetComplexEventChunk(MetaStreamEvent metaStreamEvent) {
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getOutputData().size(),
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

    @Override
    public TYPE getType() {
        return TYPE.SELECT;
    }
}

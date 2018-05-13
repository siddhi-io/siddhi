/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query;

import org.wso2.siddhi.core.aggregation.AggregationRuntime;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.state.StateEventPool;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.StoreQueryRuntimeException;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.table.CompiledUpdateSet;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.window.Window;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.List;

/**
 * Store Query Runtime holds the runtime information needed for executing the store query.
 */
public class UpdateStoreQueryRuntime implements StoreQueryRuntime {

    private CompiledCondition compiledCondition;
    private CompiledUpdateSet compiledUpdateSet;
    private Table table;
    private Window window;
    private String queryName;
    private MetaStreamEvent.EventType eventType;
    private AggregationRuntime aggregation;
    private QuerySelector selector;
    private StateEventPool stateEventPool;
    private MetaStreamEvent metaStreamEvent;
    private Attribute[] outputAttributes;

    public UpdateStoreQueryRuntime(Table table, CompiledCondition compiledCondition,
                                   String queryName, MetaStreamEvent metaStreamEvent) {
        this.table = table;
        this.compiledCondition = compiledCondition;
        this.queryName = queryName;
        this.eventType = metaStreamEvent.getEventType();
        this.metaStreamEvent = metaStreamEvent;
        this.setOutputAttributes(metaStreamEvent.getLastInputDefinition().getAttributeList());
    }

    @Override
    public Event[] execute() {
        try {
        StateEvent stateEvent = new StateEvent(1, metaStreamEvent.getOutputData().size());
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(),
                metaStreamEvent.getOutputData().size());
        StreamEvent streamEvents = null;
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        stateEvent.addEvent(0, streamEvent);
        complexEventChunk.add(stateEvent);
            switch (eventType) {
                case TABLE:
                    selector.process(complexEventChunk);
                    break;
                case DEFAULT:
                    break;
            }
            return new Event[]{};
        } catch (Throwable t) {
            throw new StoreQueryRuntimeException("Error executing '" + queryName + "', " + t.getMessage(), t);
        }
    }

    @Override
    public void reset() {
        if (selector != null) {
            selector.process(generateResetComplexEventChunk(metaStreamEvent));
        }
    }

    private ComplexEventChunk<ComplexEvent> generateResetComplexEventChunk(MetaStreamEvent metaStreamEvent) {
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setType(ComplexEvent.Type.RESET);

        StateEvent stateEvent = stateEventPool.borrowEvent();
        if (eventType == MetaStreamEvent.EventType.AGGREGATE) {
            stateEvent.addEvent(1, streamEvent);
        } else {
            stateEvent.addEvent(0, streamEvent);
        }
        stateEvent.setType(ComplexEvent.Type.RESET);

        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>(true);
        complexEventChunk.add(stateEvent);
        return complexEventChunk;
    }

    public void setStateEventPool(StateEventPool stateEventPool) {
        this.stateEventPool = stateEventPool;
    }

    public void setSelector(QuerySelector selector) {
        this.selector = selector;
    }

    /**
     * This method sets the output attribute list of the given store query.
     *
     * @param outputAttributeList
     */
    public void setOutputAttributes(List<Attribute> outputAttributeList) {
        this.outputAttributes = outputAttributeList.toArray(new Attribute[outputAttributeList.size()]);
    }

    @Override
    public Attribute[] getStoreQueryOutputAttributes() {
        return Arrays.copyOf(outputAttributes, outputAttributes.length);
    }
}

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

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.state.StateEventPool;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.StoreQueryRuntimeException;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.List;


/**
 * This class is used to keep the information needed to execute an updateOrInsert store query.
 */
public class UpdateOrInsertStoreQueryRuntime implements StoreQueryRuntime {

    private String queryName;
    private MetaStreamEvent.EventType eventType;
    private QuerySelector selector;
    private StateEventPool stateEventPool;
    private MetaStreamEvent metaStreamEvent;
    private Attribute[] outputAttributes;

    public UpdateOrInsertStoreQueryRuntime(String queryName, MetaStreamEvent metaStreamEvent) {
        this.queryName = queryName;
        this.eventType = metaStreamEvent.getEventType();
        this.metaStreamEvent = metaStreamEvent;
        this.setOutputAttributes(metaStreamEvent.getLastInputDefinition().getAttributeList());
    }

    @Override
    public Event[] execute() {
        try {
            StateEvent stateEvent = new StateEvent(1, outputAttributes.length);
            StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                    metaStreamEvent.getOnAfterWindowData().size(),
                    metaStreamEvent.getOutputData().size());
            stateEvent.addEvent(0, streamEvent);

            ComplexEventChunk complexEventChunk = new ComplexEventChunk(stateEvent, stateEvent, true);

            if (eventType == MetaStreamEvent.EventType.TABLE) {
                selector.process(complexEventChunk);
            } else {
                throw new StoreQueryRuntimeException("Update or insert store query consumes only stream events of " +
                        "type \"TABLE\".");
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

    @Override
    public TYPE getType() {
        return TYPE.UPDATE_OR_INSERT;
    }

    /**
     * This method sets a state event pool for the updateOrInsert query runtime.
     *
     * @param stateEventPool stateEventPool for the store query runtime
     */
    public void setStateEventPool(StateEventPool stateEventPool) {
        if (stateEventPool != null) {
            this.stateEventPool = stateEventPool;
        } else {
            throw new StoreQueryRuntimeException("Error occured while initializing run time for update or insert into" +
                    " query \"" + queryName + "\".");
        }
    }

    /**
     * This method sets selector for the updateOrInsert store query runtime.
     *
     * @param selector for the store query
     */
    public void setSelector(QuerySelector selector) {
        this.selector = selector;
    }

    /**
     * This method sets the output attribute list of the given store query.
     *
     * @param outputAttributeList output attributes of the store query
     */
    public void setOutputAttributes(List<Attribute> outputAttributeList) {
        this.outputAttributes = outputAttributeList.toArray(new Attribute[outputAttributeList.size()]);
    }

    @Override
    public Attribute[] getStoreQueryOutputAttributes() {
        return Arrays.copyOf(outputAttributes, outputAttributes.length);
    }

    private ComplexEventChunk<ComplexEvent> generateResetComplexEventChunk(MetaStreamEvent metaStreamEvent) {
        StreamEvent streamEvent = new StreamEvent(metaStreamEvent.getBeforeWindowData().size(),
                metaStreamEvent.getOnAfterWindowData().size(), metaStreamEvent.getOutputData().size());
        streamEvent.setType(ComplexEvent.Type.RESET);

        StateEvent stateEvent = stateEventPool.borrowEvent();
        stateEvent.addEvent(0, streamEvent);
        stateEvent.setType(ComplexEvent.Type.RESET);

        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>(true);
        complexEventChunk.add(stateEvent);
        return complexEventChunk;
    }
}

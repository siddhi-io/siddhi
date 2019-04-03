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

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.StoreQueryRuntimeException;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.List;

/**
 * Store Query Runtime Interface
 */
public abstract class StoreQueryRuntime {
    String queryName;
    MetaStreamEvent.EventType eventType;
    QuerySelector selector;
    StateEventFactory stateEventFactory;
    MetaStreamEvent metaStreamEvent;
    Attribute[] outputAttributes;

    /**
     * This method initiates the execution of store query.
     *
     * @return an array of Events.
     */
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
                throw new StoreQueryRuntimeException("DELETE, INSERT, UPDATE and UPDATE OR INSERT store query " +
                        "operations consume only stream events of type \"TABLE\".");
            }
            return new Event[]{};
        } catch (Throwable t) {
            throw new StoreQueryRuntimeException("Error executing '" + queryName + "', " + t.getMessage(), t);
        }
    }

    /**
     * This method sets a state event pool for store query runtime.
     *
     * @param stateEventFactory stateEventFactory for the store query runtime
     */
    public void setStateEventFactory(StateEventFactory stateEventFactory) {
        this.stateEventFactory = stateEventFactory;
    }

    /**
     * This method sets the output attribute list of the given store query.
     *
     * @param outputAttributeList of the store query
     */
    public void setOutputAttributes(List<Attribute> outputAttributeList) {
        this.outputAttributes = outputAttributeList.toArray(new Attribute[outputAttributeList.size()]);
    }

    /**
     * This method will return the output attributes name and its types.
     *
     * @return List of output attributes
     */
    public Attribute[] getStoreQueryOutputAttributes() {
        return Arrays.copyOf(outputAttributes, outputAttributes.length);
    }

    /**
     * This method sets selector for the delete store query runtime.
     *
     * @param selector for the store query
     */
    public void setSelector(QuerySelector selector) {
        this.selector = selector;
    }

    /**
     * This method is used to execute a store query when there is already store query runtime for that query.
     */
    public abstract void reset();

    /**
     * This method will return the type of the store query runtime.
     *
     * @return type of store query runtime. (one of the types DELETE, INSERT, SELECT, UPDATE, FIND or UPDATE OR INSERT)
     */
    public abstract TYPE getType();

    /**
     * This enum contains the possible types of the store query runtimes
     */
    enum TYPE {
        DELETE,
        INSERT,
        SELECT,
        UPDATE,
        UPDATE_OR_INSERT,
        FIND
    }
}

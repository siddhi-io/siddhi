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

package io.siddhi.core.query.output.callback;

import io.siddhi.core.debugger.SiddhiDebugger;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;

/**
 * Implementation of {@link OutputCallback} to receive processed Siddhi events from
 * Siddhi queries and insert/update data into a {@link Table}
 * based on received events and condition.
 */
public class UpdateOrInsertTableCallback extends OutputCallback {
    private final int matchingStreamIndex;
    private final AddingStreamEventExtractor addingStreamEventExtractor;
    private Table table;
    private CompiledCondition compiledCondition;
    private CompiledUpdateSet compiledUpdateSet;
    private boolean convertToStreamEvent;
    private StateEventFactory stateEventFactory;
    private StreamEventFactory streamEventFactory;
    private StreamEventConverter streamEventConverter;

    public UpdateOrInsertTableCallback(Table table, CompiledCondition compiledCondition,
                                       CompiledUpdateSet compiledUpdateSet,
                                       int matchingStreamIndex, boolean convertToStreamEvent,
                                       StateEventFactory stateEventFactory, StreamEventFactory streamEventFactory,
                                       StreamEventConverter streamEventConverter, String queryName) {
        super(queryName);
        this.matchingStreamIndex = matchingStreamIndex;
        this.table = table;
        this.compiledCondition = compiledCondition;
        this.compiledUpdateSet = compiledUpdateSet;
        this.convertToStreamEvent = convertToStreamEvent;
        this.stateEventFactory = stateEventFactory;
        this.streamEventFactory = streamEventFactory;
        this.streamEventConverter = streamEventConverter;
        this.addingStreamEventExtractor = new AddingStreamEventExtractor(matchingStreamIndex);
    }

    @Override
    public void send(ComplexEventChunk updateOrAddEventChunk, int noOfEvents) {
        if (getSiddhiDebugger() != null) {
            getSiddhiDebugger().checkBreakPoint(getQueryName(),
                    SiddhiDebugger.QueryTerminal.OUT, updateOrAddEventChunk.getFirst());
        }
        updateOrAddEventChunk.reset();
        if (updateOrAddEventChunk.hasNext()) {
            ComplexEventChunk<StateEvent> updateOrAddStateEventChunk = constructMatchingStateEventChunk
                    (updateOrAddEventChunk, convertToStreamEvent, stateEventFactory,
                            matchingStreamIndex, streamEventFactory, streamEventConverter);
            constructMatchingStateEventChunk(updateOrAddEventChunk, convertToStreamEvent, stateEventFactory,
                    matchingStreamIndex, streamEventFactory, streamEventConverter);
            table.updateOrAddEvents(updateOrAddStateEventChunk, compiledCondition, compiledUpdateSet,
                    addingStreamEventExtractor, noOfEvents);
        }
    }
}

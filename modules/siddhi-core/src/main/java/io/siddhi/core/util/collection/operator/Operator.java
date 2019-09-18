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

package io.siddhi.core.util.collection.operator;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.table.InMemoryCompiledUpdateSet;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;

/**
 * Interface for Operators related to collection of events. These will be used by in-memory table implementation.
 */
//todo support clean method
public interface Operator extends CompiledCondition {

    StreamEvent find(StateEvent matchingEvent, Object storeEvents, StreamEventCloner storeEventCloner);

    boolean contains(StateEvent matchingEvent, Object storeEvents);

    void delete(ComplexEventChunk<StateEvent> deletingEventChunk, Object storeEvents);

    void update(ComplexEventChunk<StateEvent> updatingEventChunk, Object storeEvents,
                InMemoryCompiledUpdateSet compiledUpdateSet);

    ComplexEventChunk<StateEvent> tryUpdate(ComplexEventChunk<StateEvent> updatingOrAddingEventChunk,
                                            Object storeEvents,
                                            InMemoryCompiledUpdateSet compiledUpdateSet,
                                            AddingStreamEventExtractor addingStreamEventExtractor);
}

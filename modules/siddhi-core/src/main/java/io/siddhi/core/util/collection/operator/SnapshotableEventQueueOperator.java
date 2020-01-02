/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.table.InMemoryCompiledUpdateSet;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;

import java.util.Map;

/**
 * Operator which is related to non-indexed In-memory table operations.
 */
public class SnapshotableEventQueueOperator implements Operator {
    protected ExpressionExecutor expressionExecutor;
    protected int storeEventPosition;

    public SnapshotableEventQueueOperator(ExpressionExecutor expressionExecutor, int storeEventPosition) {
        this.expressionExecutor = expressionExecutor;
        this.storeEventPosition = storeEventPosition;
    }

    public ExpressionExecutor getExpressionExecutor() {
        return expressionExecutor;
    }

    public int getStoreEventPosition() {
        return storeEventPosition;
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, Object storeEvents, StreamEventCloner storeEventCloner) {
        SnapshotableStreamEventQueue storeEventQueue = (SnapshotableStreamEventQueue) storeEvents;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<>();

        storeEventQueue.reset();
        while (storeEventQueue.hasNext()) {
            StreamEvent storeEvent = storeEventQueue.next();
            matchingEvent.setEvent(storeEventPosition, storeEvent);
            if ((Boolean) expressionExecutor.execute(matchingEvent)) {
                returnEventChunk.add(storeEventCloner.copyStreamEvent(storeEvent));
            }
            matchingEvent.setEvent(storeEventPosition, null);
        }
        return returnEventChunk.getFirst();

    }

    @Override
    public boolean contains(StateEvent matchingEvent, Object storeEvents) {
        SnapshotableStreamEventQueue storeEventQueue = (SnapshotableStreamEventQueue) storeEvents;
        try {
            storeEventQueue.reset();
            while (storeEventQueue.hasNext()) {
                StreamEvent storeEvent = storeEventQueue.next();
                matchingEvent.setEvent(storeEventPosition, storeEvent);
                if ((Boolean) expressionExecutor.execute(matchingEvent)) {
                    return true;
                }
            }
            return false;
        } finally {
            matchingEvent.setEvent(storeEventPosition, null);
        }
    }

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, Object storeEvents) {
        SnapshotableStreamEventQueue storeEventQueue = (SnapshotableStreamEventQueue) storeEvents;
        deletingEventChunk.reset();
        while (deletingEventChunk.hasNext()) {
            StateEvent deletingEvent = deletingEventChunk.next();
            try {
                storeEventQueue.reset();
                while (storeEventQueue.hasNext()) {
                    StreamEvent storeEvent = storeEventQueue.next();
                    deletingEvent.setEvent(storeEventPosition, storeEvent);
                    if ((Boolean) expressionExecutor.execute(deletingEvent)) {
                        storeEventQueue.remove();
                    }
                }
            } finally {
                deletingEvent.setEvent(storeEventPosition, null);
            }
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, Object storeEvents,
                       InMemoryCompiledUpdateSet compiledUpdateSet) {
        SnapshotableStreamEventQueue storeEventQueue = (SnapshotableStreamEventQueue) storeEvents;
        updatingEventChunk.reset();
        while (updatingEventChunk.hasNext()) {
            StateEvent updatingEvent = updatingEventChunk.next();
            try {
                storeEventQueue.reset();
                while (storeEventQueue.hasNext()) {
                    StreamEvent storeEvent = storeEventQueue.next();
                    updatingEvent.setEvent(storeEventPosition, storeEvent);
                    if ((Boolean) expressionExecutor.execute(updatingEvent)) {
                        for (Map.Entry<Integer, ExpressionExecutor> entry :
                                compiledUpdateSet.getExpressionExecutorMap().entrySet()) {
                            Object value = entry.getValue().execute(updatingEvent);
                            storeEvent.setOutputData(value, entry.getKey());
                        }
                        storeEventQueue.overwrite(storeEvent);
                    }
                }
            } finally {
                updatingEvent.setEvent(storeEventPosition, null);
            }
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> tryUpdate(ComplexEventChunk<StateEvent> updatingOrAddingEventChunk,
                                                   Object storeEvents,
                                                   InMemoryCompiledUpdateSet compiledUpdateSet,
                                                   AddingStreamEventExtractor addingStreamEventExtractor) {
        SnapshotableStreamEventQueue storeEventQueue = (SnapshotableStreamEventQueue) storeEvents;
        updatingOrAddingEventChunk.reset();
        ComplexEventChunk<StateEvent> failedEventChunk = new ComplexEventChunk<>();
        while (updatingOrAddingEventChunk.hasNext()) {
            StateEvent overwritingOrAddingEvent = updatingOrAddingEventChunk.next();
            try {
                boolean updated = false;
                storeEventQueue.reset();
                while (storeEventQueue.hasNext()) {
                    StreamEvent storeEvent = storeEventQueue.next();
                    overwritingOrAddingEvent.setEvent(storeEventPosition, storeEvent);
                    if ((Boolean) expressionExecutor.execute(overwritingOrAddingEvent)) {
                        for (Map.Entry<Integer, ExpressionExecutor> entry :
                                compiledUpdateSet.getExpressionExecutorMap().entrySet()) {
                            storeEvent.setOutputData(entry.getValue().
                                    execute(overwritingOrAddingEvent), entry.getKey());
                        }
                        storeEventQueue.overwrite(storeEvent);
                        updated = true;
                    }
                }
                if (!updated) {
                    updatingOrAddingEventChunk.remove();
                    failedEventChunk.add(overwritingOrAddingEvent);
                }
            } finally {
                overwritingOrAddingEvent.setEvent(storeEventPosition, null);
            }
        }
        return failedEventChunk;
    }
}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.table;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.table.holder.IndexEventHolder;
import io.siddhi.core.table.holder.IndexedEventHolder;
import io.siddhi.core.table.holder.ListEventHolder;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.Operator;

import static io.siddhi.core.util.cache.CacheUtils.getPrimaryKey;
import static io.siddhi.core.util.cache.CacheUtils.getPrimaryKeyFromMatchingEvent;

/**
 * cache table with FIFO entry removal
 */
public class CacheTableLFU extends InMemoryTable implements CacheTable {

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        TableState state = stateHolder.getState();
        readWriteLock.readLock().lock();
        try {
            StreamEvent foundEvent = ((Operator) compiledCondition).find(matchingEvent, state.getEventHolder(),
                    tableStreamEventCloner);
            String primaryKey;

            if (stateHolder.getState().getEventHolder() instanceof IndexEventHolder) {
                IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
                primaryKey = getPrimaryKey(compiledCondition, matchingEvent);
                StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
                if (usedEvent != null) {
                    usedEvent.getOutputData()[foundEvent.getOutputData().length - 1] =
                            (int) usedEvent.getOutputData()[foundEvent.getOutputData().length - 1] + 1;
                }
            }
            return foundEvent;
        } finally {
            stateHolder.returnState(state);
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        readWriteLock.readLock().lock();
        TableState state = stateHolder.getState();
        try {
            if (((Operator) compiledCondition).contains(matchingEvent, state.getEventHolder())) {
                String primaryKey;

                if (stateHolder.getState().getEventHolder() instanceof IndexEventHolder) {
                    IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
                    primaryKey = getPrimaryKey(compiledCondition, matchingEvent);
                    if (primaryKey == null || primaryKey.equals("")) {
                        primaryKey = getPrimaryKeyFromMatchingEvent(matchingEvent);
                    }
                    StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
                    if (usedEvent != null) {
                        usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] =
                                (int) usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] + 1;
                    }
                }
                return true;
            } else {
                return false;
            }
        } finally {
            stateHolder.returnState(state);
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            String primaryKey;
            if (stateHolder.getState().getEventHolder() instanceof IndexEventHolder) {
                for (StateEvent matchingEvent: updatingEventChunk.toList()) {
                    IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
                    primaryKey = getPrimaryKey(compiledCondition, matchingEvent);
                    StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
                    if (usedEvent != null) {
                        usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] =
                                (int) usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] + 1;
                    }
                }
            }
            ((Operator) compiledCondition).update(updatingEventChunk, state.getEventHolder(),
                    (InMemoryCompiledUpdateSet) compiledUpdateSet);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void updateOrAddWithMaxSize(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                       CompiledCondition compiledCondition,
                                       CompiledUpdateSet compiledUpdateSet,
                                       AddingStreamEventExtractor addingStreamEventExtractor, int maxTableSize) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            ComplexEventChunk<StreamEvent> failedEvents = ((Operator) compiledCondition).tryUpdate(
                    updateOrAddingEventChunk,
                    state.getEventHolder(),
                    (InMemoryCompiledUpdateSet) compiledUpdateSet,
                    addingStreamEventExtractor);
            if (failedEvents != null && failedEvents.getFirst() != null) {
                int tableSize = this.size();
                ComplexEventChunk<StreamEvent> failedEventsLimitCopy = new ComplexEventChunk<>();
                failedEvents.reset();
                while (true) {
                    failedEventsLimitCopy.add(failedEvents.next());
                    tableSize++;
                    if (tableSize == maxTableSize || !failedEvents.hasNext()) {
                        break;
                    }
                }
                state.getEventHolder().add(failedEventsLimitCopy);
            }
            String primaryKey;
            if (stateHolder.getState().getEventHolder() instanceof IndexEventHolder) {
                for (StateEvent matchingEvent: updateOrAddingEventChunk.toList()) {
                    IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
                    primaryKey = getPrimaryKey(compiledCondition, matchingEvent);
                    StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
                    if (usedEvent != null) {
                        usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] =
                                (int) usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] + 1;
                    }
                }
            }
            while (this.size() > maxTableSize) {
                this.deleteOneEntryUsingCachePolicy();
            }
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteOneEntryUsingCachePolicy() {
        if (stateHolder.getState().getEventHolder() instanceof IndexedEventHolder) {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
            Object[] keys = indexEventHolder.getAllPrimaryKeyValues().toArray();

            int minCount = Integer.MAX_VALUE;
            Object keyOfMinCount = null;

            for (Object key : keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                int count = (int) data[data.length - 1];
                if (count < minCount) {
                    minCount = count;
                    keyOfMinCount = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinCount);
        }
    }
}

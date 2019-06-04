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

import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.table.holder.IndexEventHolder;
import io.siddhi.core.util.collection.executor.AndMultiPrimaryKeyCollectionExecutor;
import io.siddhi.core.util.collection.executor.CompareCollectionExecutor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.collection.operator.OverwriteTableIndexOperator;

import java.util.List;

/**
 * cache table with LRU entry removal
 */
public class CacheTableLRU extends InMemoryTable implements CacheTable {

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        TableState state = stateHolder.getState();
        readWriteLock.readLock().lock();
        try {
            StreamEvent foundEvent = ((Operator) compiledCondition).find(matchingEvent, state.eventHolder,
                    tableStreamEventCloner);
            //todo: update last used time
            String primaryKey;

            if (stateHolder.getState().eventHolder instanceof IndexEventHolder) {
                IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
                primaryKey = getPrimaryKeyFromCompileCondition(compiledCondition);
                if (foundEvent != null) {
                    StreamEvent foundEventCopy = foundEvent.clone();
                    foundEventCopy.getOutputData()[foundEvent.getOutputData().length - 1] = System.currentTimeMillis();
                    indexEventHolder.replace(primaryKey, foundEventCopy);
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
            if (((Operator) compiledCondition).contains(matchingEvent, state.eventHolder)) {
                //todo: update last used time
                String primaryKey;

                if (stateHolder.getState().eventHolder instanceof IndexEventHolder) {
                    IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
                    primaryKey = getPrimaryKeyFromCompileCondition(compiledCondition);
                    if (primaryKey == null || primaryKey.equals("")) {
                        primaryKey = getPrimaryKeyFromMatchingEvent(matchingEvent);
                    }
                    StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
//                    StreamEvent foundEventCopy = foundEvent.clone();
                    if (usedEvent != null) {
                        usedEvent.getOutputData()[usedEvent.getOutputData().length - 1] = System.currentTimeMillis();
                        indexEventHolder.replace(primaryKey, usedEvent); // todo not needed. pass by reference
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

    private String getPrimaryKeyFromMatchingEvent(StateEvent matchingEvent) {
        StringBuilder primaryKey = new StringBuilder();
        StreamEvent streamEvent = matchingEvent.getStreamEvent(0);
        Object[] data = streamEvent.getOutputData();
        for (Object object: data) {
            primaryKey.append(object);
            primaryKey.append(":-:");
        }
        return primaryKey.toString();
    }

    private String getPrimaryKeyFromCompileCondition(CompiledCondition compiledCondition) {
        StringBuilder primaryKey = new StringBuilder();
        if (compiledCondition instanceof OverwriteTableIndexOperator) {
            OverwriteTableIndexOperator operator = (OverwriteTableIndexOperator) compiledCondition;

            if (operator.getCollectionExecutor() instanceof AndMultiPrimaryKeyCollectionExecutor) {
                AndMultiPrimaryKeyCollectionExecutor executor = (AndMultiPrimaryKeyCollectionExecutor) operator.
                        getCollectionExecutor();
//                primaryKey.append(executor.getCompositePrimaryKey());
                List<ExpressionExecutor> lis = executor.getMultiPrimaryKeyExpressionExecutors();
                for (ExpressionExecutor ee : lis) {
                    if (ee instanceof ConstantExpressionExecutor) {
                        primaryKey.append(((ConstantExpressionExecutor) ee).getValue());
                        primaryKey.append(":-:");
                    }
                }
            } else if (operator.getCollectionExecutor() instanceof CompareCollectionExecutor) {
                CompareCollectionExecutor executor = (CompareCollectionExecutor) operator.getCollectionExecutor();
                if (executor.getValueExpressionExecutor() instanceof ConstantExpressionExecutor) {
                    primaryKey.append(((ConstantExpressionExecutor) executor.getValueExpressionExecutor()).getValue());
                } else {
                    return null;
                }
            }
            return primaryKey.toString();
        } else {
            return null;
        }
    }

    @Override
    public void deleteOneEntryUsingCachePolicy() { //todo: chage from fifo to lru logic
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
            Object[] keys = indexEventHolder.getAllPrimaryKeyValues().toArray();

            long minTimestamp = Long.MAX_VALUE;
            Object keyOfMinTimestamp = null;

            for (Object key: keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                long timestamp = (long) data[data.length - 1];
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                    keyOfMinTimestamp = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinTimestamp);

        } catch (ClassCastException ignored) {

        }
    }
}

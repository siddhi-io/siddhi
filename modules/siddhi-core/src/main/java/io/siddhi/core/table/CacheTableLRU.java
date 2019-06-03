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

            try {
                IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
                primaryKey = getPrimaryKeyFromCompileCondition(compiledCondition);
                if (foundEvent != null) {
                    StreamEvent foundEventCopy = foundEvent.clone();
                    foundEventCopy.getOutputData()[foundEvent.getOutputData().length - 1] = System.currentTimeMillis();
                    indexEventHolder.replace(primaryKey, foundEventCopy);
                }
            } catch (ClassCastException ignored) {

            }

            return foundEvent;
        } finally {
            stateHolder.returnState(state);
            readWriteLock.readLock().unlock();
        }
    }

    private String getPrimaryKeyFromCompileCondition(CompiledCondition compiledCondition) {
        StringBuilder primaryKey = new StringBuilder();
        try {
            OverwriteTableIndexOperator operator = (OverwriteTableIndexOperator) compiledCondition;
            AndMultiPrimaryKeyCollectionExecutor executor = (AndMultiPrimaryKeyCollectionExecutor) operator.
                    getCollectionExecutor();
            List<ExpressionExecutor> lis =  executor.getMultiPrimaryKeyExpressionExecutors();
            for (ExpressionExecutor ee: lis) {
                primaryKey.append(((ConstantExpressionExecutor) ee).getValue());
                primaryKey.append(":-:");
            }
        } catch (ClassCastException e) {

        }
        return primaryKey.toString();
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

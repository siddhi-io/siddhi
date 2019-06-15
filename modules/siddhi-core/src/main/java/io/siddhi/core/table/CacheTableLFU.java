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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.table.holder.IndexEventHolder;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Set;

import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_COUNT_LFU;
import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_TIMESTAMP_ADDED;
import static io.siddhi.core.util.cache.CacheUtils.getPrimaryKey;
import static io.siddhi.core.util.cache.CacheUtils.getPrimaryKeyFromMatchingEvent;

/**
 * cache table with FIFO entry removal
 */
public class CacheTableLFU extends CacheTable {
    private static final Logger log = Logger.getLogger(CacheTableLFU.class);

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        //todo: use index operator to update
        readWriteLock.readLock().lock();
        TableState state = stateHolder.getState();
        try {
            StreamEvent foundEvent = ((Operator) compiledCondition).find(matchingEvent, state.getEventHolder(),
                    tableStreamEventCloner);

            if (stateHolder.getState().getEventHolder() instanceof IndexEventHolder && foundEvent != null) {
                //todo: check if CC meets required criteria for cache
                foundEvent.getOutputData()[cachePolicyAttributePosition] =
                        (int) foundEvent.getOutputData()[cachePolicyAttributePosition] + 1;
                ((IndexEventHolder) stateHolder.getState().getEventHolder()).overwrite(foundEvent);
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
                    //todo: use index operator or smthn
                    primaryKey = getPrimaryKey(compiledCondition, matchingEvent);
                    if (primaryKey == null || primaryKey.equals("")) {
                        primaryKey = getPrimaryKeyFromMatchingEvent(matchingEvent);
                    }
                    StreamEvent usedEvent = indexEventHolder.getEvent(primaryKey);
                    if (usedEvent != null) {
                        usedEvent.getOutputData()[cachePolicyAttributePosition] =
                                (int) usedEvent.getOutputData()[cachePolicyAttributePosition] + 1;
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
    void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition, boolean cacheExpiryEnabled) {
        if (cacheExpiryEnabled) {
            cacheTableDefinition.attribute(CACHE_TABLE_TIMESTAMP_ADDED, Attribute.Type.LONG);
            cacheTableDefinition.attribute(CACHE_TABLE_COUNT_LFU, Attribute.Type.INT);
            expiryAttributePosition = cacheTableDefinition.getAttributeList().size() - 2;
        } else {
            cacheTableDefinition.attribute(CACHE_TABLE_COUNT_LFU, Attribute.Type.INT);
        }
        cachePolicyAttributePosition = cacheTableDefinition.getAttributeList().size() - 1;
        numColumns = cachePolicyAttributePosition + 1;
    }

    @Override
    public void deleteOneEntryUsingCachePolicy() {
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
            Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
            int minCount = Integer.MAX_VALUE;
            Object keyOfMinCount = null;
            for (Object key : keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                int count = (int) data[cachePolicyAttributePosition];
                if (count < minCount) {
                    minCount = count;
                    keyOfMinCount = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinCount);
        } catch (ClassCastException e) {
            log.error(siddhiAppContext + ": " + e.getMessage());
        }
    }

    @Override
    public void deleteEntriesUsingCachePolicy(int numRowsToDelete) {
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
            Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
            int[] minCountArray = new int[numRowsToDelete];
            Arrays.fill(minCountArray, Integer.MAX_VALUE);
            Object[] keyOfMinCountArray = new Object[numRowsToDelete];

            for (Object key: keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                int count = (int) data[cachePolicyAttributePosition];
                for (int i = 0; i < numRowsToDelete; i++) {
                    if (count < minCountArray[i]) {
                        minCountArray[i] = count;
                        keyOfMinCountArray[i] = key;
                    }
                }
            }
            for (Object deleteKey: keyOfMinCountArray) {
                if (deleteKey != null) {
                    indexEventHolder.deleteEvent(deleteKey);
                }
            }
        } catch (ClassCastException e) {
            log.error(siddhiAppContext + ": " + e.getMessage());
        }
    }

    @Override
    protected StreamEvent addRequiredFields(ComplexEvent event, SiddhiAppContext siddhiAppContext,
                                            boolean cacheExpiryEnabled) {
        Object[] outputDataForCache;
        Object[] outputData = event.getOutputData();
        if (cacheExpiryEnabled) {
            outputDataForCache = new Object[numColumns];
            outputDataForCache[expiryAttributePosition] =
                    siddhiAppContext.getTimestampGenerator().currentTime();
            outputDataForCache[cachePolicyAttributePosition] = 1;
        } else {
            outputDataForCache = new Object[numColumns];
            outputDataForCache[cachePolicyAttributePosition] = 1;
        }
        System.arraycopy(outputData, 0 , outputDataForCache, 0, outputData.length);
        StreamEvent eventForCache = new StreamEvent(0, 0, outputDataForCache.length);
        eventForCache.setOutputData(outputDataForCache);
        return eventForCache;
    }

    @Override
    public void updateCachePolicyAttribute(StreamEvent streamEvent) {
        streamEvent.getOutputData()[cachePolicyAttributePosition] =
                (int) streamEvent.getOutputData()[cachePolicyAttributePosition] + 1;
    }
}

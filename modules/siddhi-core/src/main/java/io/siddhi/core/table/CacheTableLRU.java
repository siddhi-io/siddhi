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
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.table.holder.IndexEventHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Set;

import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_TIMESTAMP_ADDED;
import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_TIMESTAMP_LRU;

/**
 * cache table with LRU entry removal
 */
public class CacheTableLRU extends CacheTable {
    private static final Logger log = Logger.getLogger(CacheTableLRU.class);

    @Override
    void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition, boolean cacheExpiryEnabled) {
        if (cacheExpiryEnabled) {
            cacheTableDefinition.attribute(CACHE_TABLE_TIMESTAMP_ADDED, Attribute.Type.LONG);
            cacheTableDefinition.attribute(CACHE_TABLE_TIMESTAMP_LRU, Attribute.Type.LONG);
            expiryAttributePosition = cacheTableDefinition.getAttributeList().size() - 2;
        } else {
            cacheTableDefinition.attribute(CACHE_TABLE_TIMESTAMP_LRU, Attribute.Type.LONG);
        }
        cachePolicyAttributePosition = cacheTableDefinition.getAttributeList().size() - 1;
        numColumns = cachePolicyAttributePosition + 1;
    }

    @Override
    public void deleteOneEntryUsingCachePolicy() {
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
            Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
            long minTimestamp = Long.MAX_VALUE;
            Object keyOfMinTimestamp = null;
            for (Object key: keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                long timestamp = (long) data[cachePolicyAttributePosition];
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                    keyOfMinTimestamp = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinTimestamp);
        } catch (ClassCastException e) {
            log.error(siddhiAppContext + ": " + e.getMessage());
        }
    }

    @Override
    public void deleteEntriesUsingCachePolicy(int numRowsToDelete) {
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
            Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
            long[] minTimestampArray = new long[numRowsToDelete];
            Arrays.fill(minTimestampArray, Long.MAX_VALUE);
            Object[] keyOfMinTimestampArray = new Object[numRowsToDelete];

            for (Object key: keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                long timestamp = (long) data[cachePolicyAttributePosition];
                for (int i = 0; i < numRowsToDelete; i++) {
                    if (timestamp < minTimestampArray[i]) {
                        minTimestampArray[i] = timestamp;
                        keyOfMinTimestampArray[i] = key;
                    }
                }
            }
            for (Object deleteKey: keyOfMinTimestampArray) {
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
            outputDataForCache[expiryAttributePosition] = outputDataForCache[cachePolicyAttributePosition] =
                            siddhiAppContext.getTimestampGenerator().currentTime();
        } else {
            outputDataForCache = new Object[numColumns];
            outputDataForCache[cachePolicyAttributePosition] =
                    siddhiAppContext.getTimestampGenerator().currentTime();
        }
        System.arraycopy(outputData, 0 , outputDataForCache, 0, outputData.length);
        StreamEvent eventForCache = new StreamEvent(0, 0, outputDataForCache.length);
        eventForCache.setOutputData(outputDataForCache);
        return eventForCache;
    }

    @Override
    public void updateCachePolicyAttribute(StreamEvent streamEvent) {
        streamEvent.getOutputData()[cachePolicyAttributePosition] =
                siddhiAppContext.getTimestampGenerator().currentTime();
    }
}

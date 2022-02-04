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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.TreeMap;

import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_TIMESTAMP_ADDED;

/**
 * cache table with FIFO entry removal
 */
public class CacheTableFIFO extends CacheTable {
    private static final Logger log = LogManager.getLogger(CacheTableFIFO.class);
    private int cachePolicyAttributePosition;
    private int numColumns;

    @Override
    void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition, boolean cacheExpiryEnabled) {
        cacheTableDefinition.attribute(CACHE_TABLE_TIMESTAMP_ADDED, Attribute.Type.LONG);
        cachePolicyAttributePosition = cacheTableDefinition.getAttributeList().size() - 1;
        numColumns = cachePolicyAttributePosition + 1;
    }

    @Override
    public void deleteOneEntryUsingCachePolicy() {
        IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
        Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
        long minTimestamp = Long.MAX_VALUE;
        Object keyOfMinTimestamp = null;
        for (Object key : keys) {
            Object[] data = indexEventHolder.getEvent(key).getOutputData();
            long timestamp = (long) data[cachePolicyAttributePosition];
            if (timestamp < minTimestamp) {
                minTimestamp = timestamp;
                keyOfMinTimestamp = key;
            }
        }
        indexEventHolder.deleteEvent(keyOfMinTimestamp);
    }

    @Override
    public void deleteEntriesUsingCachePolicy(int numRowsToDelete) {
        IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().getEventHolder();
        if (numRowsToDelete >= indexEventHolder.size()) {
            indexEventHolder.deleteAll();
        } else {
            Set<Object> keys = indexEventHolder.getAllPrimaryKeyValues();
            TreeMap<Long, Object> toDelete = new TreeMap<>();
            for (Object key : keys) {
                if (toDelete.size() < numRowsToDelete) {
                    toDelete.put((Long) indexEventHolder.getEvent(key).getOutputData()[cachePolicyAttributePosition],
                            key);
                } else {
                    Long timestamp = (Long) indexEventHolder.getEvent(key).
                            getOutputData()[cachePolicyAttributePosition];
                    Long firstKey = toDelete.firstKey();
                    if (timestamp < firstKey) {
                        toDelete.remove(firstKey);
                        toDelete.put(timestamp, key);
                    }
                }
            }
            for (Object deleteKey : toDelete.values()) {
                if (deleteKey != null) {
                    indexEventHolder.deleteEvent(deleteKey);
                }
            }
        }
    }

    @Override
    protected StreamEvent addRequiredFields(ComplexEvent event, SiddhiAppContext siddhiAppContext,
                                            boolean cacheExpiryEnabled) {
        Object[] outputDataForCache;
        Object[] outputData = event.getOutputData();
        outputDataForCache = new Object[numColumns];
        outputDataForCache[cachePolicyAttributePosition] = siddhiAppContext.getTimestampGenerator().currentTime();
        System.arraycopy(outputData, 0, outputDataForCache, 0, outputData.length);
        StreamEvent eventForCache = new StreamEvent(0, 0, outputDataForCache.length);
        eventForCache.setOutputData(outputDataForCache);
        return eventForCache;
    }

    @Override
    public void updateCachePolicyAttribute(StreamEvent streamEvent) {
    }
}

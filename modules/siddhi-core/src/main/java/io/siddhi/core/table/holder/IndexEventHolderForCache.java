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
package io.siddhi.core.table.holder;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.table.CacheTable;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Exgtension of IndexEventHolder that implements hook handleCachePolicyAttributeUpdate for cache usage
 */
public class IndexEventHolderForCache extends IndexEventHolder implements IndexedEventHolder {
    private CacheTable cacheTable;

    public IndexEventHolderForCache(StreamEventFactory tableStreamEventFactory, StreamEventConverter eventConverter,
                                    PrimaryKeyReferenceHolder[] primaryKeyReferenceHolders, boolean isPrimaryNumeric,
                                    Map<String, Integer> indexMetaData, AbstractDefinition tableDefinition,
                                    SiddhiAppContext siddhiAppContext) {
        super(tableStreamEventFactory, eventConverter, primaryKeyReferenceHolders, isPrimaryNumeric, indexMetaData,
                tableDefinition, siddhiAppContext);
    }

    @Override
    protected void handleCachePolicyAttributeUpdate(StreamEvent streamEvent) {
        cacheTable.updateCachePolicyAttribute(streamEvent);
    }

    public void setCacheTable(CacheTable cacheTable) {
        this.cacheTable = cacheTable;
    }

    @Override
    public boolean containsEventSet(String attribute, Compare.Operator operator, Object value) {
        if (primaryKeyData != null && attribute.equals(primaryKeyAttributes)) {
            StreamEvent foundEvent;
            switch (operator) {
                case LESS_THAN:
                    foundEvent = (StreamEvent) ((TreeMap<Object, StreamEvent>) primaryKeyData).
                            lowerKey(value);
                    if (foundEvent != null) {
                        handleCachePolicyAttributeUpdate(foundEvent);
                        return true;
                    } else {
                        return false;
                    }
                case GREATER_THAN:
                    foundEvent = (StreamEvent) ((TreeMap<Object, StreamEvent>) primaryKeyData).
                            higherKey(value);
                    if (foundEvent != null) {
                        handleCachePolicyAttributeUpdate(foundEvent);
                        return true;
                    } else {
                        return false;
                    }
                case LESS_THAN_EQUAL:
                    foundEvent = (StreamEvent) ((TreeMap<Object, StreamEvent>) primaryKeyData).
                            ceilingKey(value);
                    if (foundEvent != null) {
                        handleCachePolicyAttributeUpdate(foundEvent);
                        return true;
                    } else {
                        return false;
                    }
                case GREATER_THAN_EQUAL:
                    foundEvent = (StreamEvent) ((TreeMap<Object, StreamEvent>) primaryKeyData).
                            floorKey(value);
                    if (foundEvent != null) {
                        handleCachePolicyAttributeUpdate(foundEvent);
                        return true;
                    } else {
                        return false;
                    }
                case EQUAL:
                    foundEvent = primaryKeyData.get(value);
                    if (foundEvent != null) {
                        handleCachePolicyAttributeUpdate(foundEvent);
                        return true;
                    } else {
                        return false;
                    }
                case NOT_EQUAL:
                    return primaryKeyData.size() > 1;
            }
        } else {
            TreeMap<Object, Set<StreamEvent>> currentIndexedData = indexData.get(attribute);

            switch (operator) {

                case LESS_THAN:
                    return currentIndexedData.lowerKey(value) != null;
                case GREATER_THAN:
                    return currentIndexedData.higherKey(value) != null;
                case LESS_THAN_EQUAL:
                    return currentIndexedData.ceilingKey(value) != null;
                case GREATER_THAN_EQUAL:
                    return currentIndexedData.floorKey(value) != null;
                case EQUAL:
                    return currentIndexedData.get(value) != null;
                case NOT_EQUAL:
                    return currentIndexedData.size() > 1;
            }
        }
        throw new OperationNotSupportedException(operator + " not supported for '" + value + "' by " + getClass()
                .getName());
    }
}

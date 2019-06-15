/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.table.CacheTable;
import io.siddhi.query.api.definition.AbstractDefinition;

import java.util.Map;

/**
 * Exgtension of IndexEventHolder that implements hook handleCachePolicyAttributeUpdate for cache usage
 */
public class IndexEventHolderForCache extends IndexEventHolder {
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
}

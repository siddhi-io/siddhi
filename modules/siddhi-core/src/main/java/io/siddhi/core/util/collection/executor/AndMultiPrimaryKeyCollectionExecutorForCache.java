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
package io.siddhi.core.util.collection.executor;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.table.CacheTable;
import io.siddhi.core.table.holder.IndexedEventHolder;

import java.util.Collection;
import java.util.List;

/**
 * extension of AndMultiPrimaryKeyCollectionExecutor that calls updateCachePolicyAttribute for cache
 */
public class AndMultiPrimaryKeyCollectionExecutorForCache extends AndMultiPrimaryKeyCollectionExecutor {
    private CacheTable cacheTable;

    public AndMultiPrimaryKeyCollectionExecutorForCache(String compositePrimaryKey,
                                                        List<ExpressionExecutor> multiPrimaryKeyExpressionExecutors) {
        super(compositePrimaryKey, multiPrimaryKeyExpressionExecutors);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder, StreamEventCloner
            storeEventCloner) {

        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        Collection<StreamEvent> storeEventSet = findEvents(matchingEvent, indexedEventHolder);

        if (storeEventSet == null) {
            return returnEventChunk.getFirst();
        } else {
            for (StreamEvent storeEvent : storeEventSet) {
                cacheTable.updateCachePolicyAttribute(storeEvent);
                if (storeEventCloner != null) {
                    returnEventChunk.add(storeEventCloner.copyStreamEvent(storeEvent));
                } else {
                    returnEventChunk.add(storeEvent);
                }
            }
            return returnEventChunk.getFirst();
        }
    }

    public void setCacheTable(CacheTable cacheTable) {
        this.cacheTable = cacheTable;
    }
}

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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
import io.siddhi.query.api.expression.condition.Compare;

import java.util.Collection;

/**
 * Implementation of {@link CollectionExecutor} which handle compare condition.
 */
public class CompareCollectionExecutor implements CollectionExecutor {
    private final String attribute;
    private final Compare.Operator operator;
    private final ExpressionExecutor valueExpressionExecutor;
    protected ExpressionExecutor expressionExecutor;
    protected int storeEventIndex;
    private CacheTable cacheTable;

    public CompareCollectionExecutor(ExpressionExecutor expressionExecutor, int storeEventIndex, String attribute,
                                     Compare.Operator operator, ExpressionExecutor valueExpressionExecutor,
                                     CacheTable cacheTable) {
        this.expressionExecutor = expressionExecutor;
        this.storeEventIndex = storeEventIndex;

        this.attribute = attribute;
        this.operator = operator;
        this.valueExpressionExecutor = valueExpressionExecutor;
        this.cacheTable = cacheTable;
    }

    public StreamEvent find(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder, StreamEventCloner
            storeEventCloner) {

        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        Collection<StreamEvent> storeEventSet = findEvents(matchingEvent, indexedEventHolder);

        if (storeEventSet == null) {
            //triggering sequential scan
            Collection<StreamEvent> storeEvents = indexedEventHolder.getAllEvents();
            for (StreamEvent storeEvent : storeEvents) {
                matchingEvent.setEvent(storeEventIndex, storeEvent);
                if ((Boolean) expressionExecutor.execute(matchingEvent)) {
                    if (storeEventCloner != null) {
                        returnEventChunk.add(storeEventCloner.copyStreamEvent(storeEvent));
                    } else {
                        returnEventChunk.add(storeEvent);
                    }
                }
                matchingEvent.setEvent(storeEventIndex, null);
            }
            return returnEventChunk.getFirst();
        } else {
            for (StreamEvent storeEvent : storeEventSet) {
                if (cacheTable != null) {
                    cacheTable.updateCachePolicyAttribute(storeEvent);
                }
                if (storeEventCloner != null) {
                    returnEventChunk.add(storeEventCloner.copyStreamEvent(storeEvent));
                } else {
                    returnEventChunk.add(storeEvent);
                }
            }
            return returnEventChunk.getFirst();
        }
    }

    public ExpressionExecutor getValueExpressionExecutor() {
        return valueExpressionExecutor;
    }

    public Collection<StreamEvent> findEvents(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder) {
        if (operator == Compare.Operator.NOT_EQUAL) {
            //for not equal trigger sequential scan
            return null;
        }
        return indexedEventHolder.findEvents(attribute, operator, valueExpressionExecutor.execute(matchingEvent));
    }

    @Override
    public boolean contains(StateEvent matchingEvent, IndexedEventHolder indexedEventHolder) {
        return indexedEventHolder.containsEventSet(attribute, operator, valueExpressionExecutor.execute(matchingEvent));
    }

    @Override
    public void delete(StateEvent deletingEvent, IndexedEventHolder indexedEventHolder) {
        indexedEventHolder.delete(attribute, operator, valueExpressionExecutor.execute(deletingEvent));
    }

    @Override
    public Cost getDefaultCost() {
        if (operator == Compare.Operator.EQUAL) {
            return Cost.SINGLE_RETURN_INDEX_MATCHING;
        } else if (operator == Compare.Operator.NOT_EQUAL) {
            return Cost.EXHAUSTIVE;
        } else {
            return Cost.MULTI_RETURN_INDEX_MATCHING;
        }
    }

    public String getAttribute() {
        return attribute;
    }

}

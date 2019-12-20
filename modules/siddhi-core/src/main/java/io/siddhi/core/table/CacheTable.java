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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.holder.EventHolder;
import io.siddhi.core.table.holder.IndexEventHolderForCache;
import io.siddhi.core.table.record.RecordTableHandler;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.EventHolderPasser;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.siddhi.query.api.util.AnnotationHelper.getAnnotation;

/**
 * common interface for FIFO, LRU, and LFU cache tables
 */
public abstract class CacheTable extends InMemoryTable {
    private int maxSize;
    private boolean cacheExpiryEnabled;

    @Override
    public void init(TableDefinition tableDefinition, StreamEventFactory storeEventPool,
                     StreamEventCloner storeEventCloner, ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                     RecordTableHandler recordTableHandler) {
        this.tableDefinition = tableDefinition;
        this.tableStreamEventCloner = storeEventCloner;
        EventHolder eventHolder = EventHolderPasser.parse(tableDefinition, storeEventPool, siddhiAppContext, true);

        if (eventHolder instanceof IndexEventHolderForCache) {
            ((IndexEventHolderForCache) eventHolder).setCacheTable(this);
        }

        stateHolder = siddhiAppContext.generateStateHolder(tableDefinition.getId(),
                () -> new TableState(eventHolder));
    }

    public void initCacheTable(TableDefinition cacheTableDefinition, ConfigReader configReader,
                               SiddhiAppContext siddhiAppContext, RecordTableHandler recordTableHandler,
                               boolean cacheExpiryEnabled, int maxSize, String cachePolicy) {
        this.maxSize = maxSize;
        this.cacheExpiryEnabled = cacheExpiryEnabled;
        this.siddhiAppContext = siddhiAppContext;
        addRequiredFieldsToCacheTableDefinition(cacheTableDefinition, cacheExpiryEnabled);

        // initialize cache table
        MetaStreamEvent cacheTableMetaStreamEvent = new MetaStreamEvent();
        cacheTableMetaStreamEvent.addInputDefinition(cacheTableDefinition);
        for (Attribute attribute : cacheTableDefinition.getAttributeList()) {
            cacheTableMetaStreamEvent.addOutputData(attribute);
        }

        StreamEventFactory cacheTableStreamEventFactory = new StreamEventFactory(cacheTableMetaStreamEvent);
        StreamEventCloner cacheTableStreamEventCloner = new StreamEventCloner(cacheTableMetaStreamEvent,
                cacheTableStreamEventFactory);
        super.initTable(cacheTableDefinition, cacheTableStreamEventFactory,
                cacheTableStreamEventCloner, configReader, siddhiAppContext, recordTableHandler);
    }

    public void addStreamEventUptoMaxSize(StreamEvent streamEvent) {
        int sizeAfterAdding = this.size();
        ComplexEventChunk<StreamEvent> addEventsLimitCopy = new ComplexEventChunk<>();
        while (sizeAfterAdding < maxSize && streamEvent != null) {
            sizeAfterAdding++;
            addEventsLimitCopy.add((StreamEvent) generateEventWithRequiredFields(streamEvent, siddhiAppContext,
                    cacheExpiryEnabled));
            streamEvent = streamEvent.getNext();
        }
        readWriteLock.writeLock().lock();
        try {
            this.add(addEventsLimitCopy);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void addAndTrimUptoMaxSize(ComplexEventChunk<StreamEvent> addingEventChunk) {
        ComplexEventChunk<StreamEvent> addingEventChunkForCache = new ComplexEventChunk<>();
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            StreamEvent event = addingEventChunk.next();
            addingEventChunkForCache.add((StreamEvent) generateEventWithRequiredFields(event, siddhiAppContext,
                    cacheExpiryEnabled));
        }
        readWriteLock.writeLock().lock();
        try {
            super.add(addingEventChunkForCache);
            if (this.size() > maxSize) {
                this.deleteEntriesUsingCachePolicy(this.size() - maxSize);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void updateOrAddAndTrimUptoMaxSize(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                              CompiledCondition compiledCondition,
                                              CompiledUpdateSet compiledUpdateSet,
                                              AddingStreamEventExtractor addingStreamEventExtractor, int maxTableSize) {
        ComplexEventChunk<StateEvent> updateOrAddingEventChunkForCache = new ComplexEventChunk<>();
        updateOrAddingEventChunk.reset();
        while (updateOrAddingEventChunk.hasNext()) {
            StateEvent event = updateOrAddingEventChunk.next();
            updateOrAddingEventChunkForCache.add((StateEvent) generateEventWithRequiredFields(event, siddhiAppContext,
                    cacheExpiryEnabled));
        }

        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            InMemoryCompiledCondition inMemoryCompiledCondition = (InMemoryCompiledCondition) compiledCondition;
            ComplexEventChunk<StateEvent> failedEvents = ((Operator) inMemoryCompiledCondition.
                    getOperatorCompiledCondition()).tryUpdate(
                    updateOrAddingEventChunkForCache,
                    state.getEventHolder(),
                    (InMemoryCompiledUpdateSet) compiledUpdateSet,
                    addingStreamEventExtractor);
            if (failedEvents != null && failedEvents.getFirst() != null) {
                state.getEventHolder().add(reduceEventsForUpdateOrInsert(
                        addingStreamEventExtractor, inMemoryCompiledCondition,
                        (InMemoryCompiledUpdateSet) compiledUpdateSet, failedEvents));
            }
            if (this.size() > maxSize) {
                this.deleteEntriesUsingCachePolicy(this.size() - maxSize);
            }
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    public void deleteAll() {
        readWriteLock.writeLock().lock();
        try {
            stateHolder.getState().getEventHolder().deleteAll();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    abstract void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition,
                                                          boolean cacheExpiryEnabled);

    public abstract void deleteOneEntryUsingCachePolicy();

    public abstract void deleteEntriesUsingCachePolicy(int numRowsToDelete);

    ComplexEvent generateEventWithRequiredFields(ComplexEvent event,
                                                 SiddhiAppContext siddhiAppContext,
                                                 boolean cacheExpiryEnabled) {
        if (event instanceof StreamEvent) {
            StreamEvent eventForCache = addRequiredFields(event, siddhiAppContext, cacheExpiryEnabled);
            return eventForCache;
        } else if (event instanceof StateEvent) {
            StreamEvent eventForCache = addRequiredFields(((StateEvent) event).getStreamEvent(0),
                    siddhiAppContext, cacheExpiryEnabled);
            StateEvent stateEvent = new StateEvent(((StateEvent) event).getStreamEvents().length,
                    eventForCache.getOutputData().length);
            stateEvent.addEvent(0, eventForCache);
            return stateEvent;
        } else {
            return null;
        }
    }

    protected abstract StreamEvent addRequiredFields(ComplexEvent event, SiddhiAppContext siddhiAppContext,
                                                     boolean cacheExpiryEnabled);

    public CacheCompiledConditionWithRouteToCache generateCacheCompileCondition(
            Expression condition, MatchingMetaInfoHolder storeMatchingMetaInfoHolder,
            SiddhiQueryContext siddhiQueryContext, List<VariableExpressionExecutor> storeVariableExpressionExecutors) {
        boolean routeToCache = checkConditionToRouteToCache(condition, storeMatchingMetaInfoHolder);
        MetaStateEvent metaStateEvent = new MetaStateEvent(storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents().length);
        for (MetaStreamEvent referenceMetaStreamEvent : storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents()) {
            metaStateEvent.addEvent(referenceMetaStreamEvent);
        }
        MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(
                metaStateEvent,
                storeMatchingMetaInfoHolder.getMatchingStreamEventIndex(),
                storeMatchingMetaInfoHolder.getStoreEventIndex(),
                storeMatchingMetaInfoHolder.getMatchingStreamDefinition(),
                this.tableDefinition,
                storeMatchingMetaInfoHolder.getCurrentState());

        Map<String, Table> tableMap = new ConcurrentHashMap<>();
        tableMap.put(this.tableDefinition.getId(), this);

        return new CacheCompiledConditionWithRouteToCache(compileCondition(condition, matchingMetaInfoHolder,
                storeVariableExpressionExecutors, tableMap, siddhiQueryContext, routeToCache), routeToCache);
    }

    private boolean checkConditionToRouteToCache(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder) {
        List<String> primaryKeysArray = new ArrayList<>();
        Annotation primaryKeys = getAnnotation("PrimaryKey", tableDefinition.getAnnotations());
        if (primaryKeys == null) {
            return false;
        }
        List<Element> keys = primaryKeys.getElements();
        for (Element element : keys) {
            primaryKeysArray.add(element.getValue());
        }
        recursivelyCheckConditionToRouteToCache(condition, primaryKeysArray, matchingMetaInfoHolder);
        return primaryKeysArray.size() == 0;
    }

    private void recursivelyCheckConditionToRouteToCache(Expression condition, List<String> primaryKeysArray,
                                                         MatchingMetaInfoHolder matchingMetaInfoHolder) {
        if (condition instanceof Compare) {
            Compare compareCondition = (Compare) condition;
            if (compareCondition.getOperator() == Compare.Operator.EQUAL) {
                if (compareCondition.getLeftExpression() instanceof Variable) {
                    Variable variable = (Variable) compareCondition.getLeftExpression();
                    if (checkIfVariableMatchesTable(variable, matchingMetaInfoHolder)) {
                        primaryKeysArray.remove(variable.getAttributeName());
                    }
                }
                if (compareCondition.getRightExpression() instanceof Variable) {
                    Variable variable = (Variable) compareCondition.getRightExpression();
                    if (checkIfVariableMatchesTable(variable, matchingMetaInfoHolder)) {
                        primaryKeysArray.remove(variable.getAttributeName());
                    }
                }
            }
        } else if (condition instanceof And) {
            recursivelyCheckConditionToRouteToCache(((And) condition).getLeftExpression(), primaryKeysArray,
                    matchingMetaInfoHolder);
            recursivelyCheckConditionToRouteToCache(((And) condition).getRightExpression(), primaryKeysArray,
                    matchingMetaInfoHolder);
        }
    }

    private boolean checkIfVariableMatchesTable(Variable variable, MatchingMetaInfoHolder matchingMetaInfoHolder) {
        if (variable.getStreamId() == null) {
            return false;
        }
        if (variable.getStreamId().equalsIgnoreCase(tableDefinition.getId())) {
            return true;
        }

        for (MetaStreamEvent streamEvent : matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvents()) {
            if (streamEvent.getInputReferenceId() != null &&
                    streamEvent.getInputReferenceId().equalsIgnoreCase(variable.getStreamId())) {
                if (streamEvent.getInputDefinitions().get(0).getId().equalsIgnoreCase(tableDefinition.getId())) {
                    return true;
                }
            }
        }
        return false;
    }

    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext,
                                              boolean updateCachePolicyAttribute) {
        TableState state = stateHolder.getState();
        try {
            return new InMemoryCompiledCondition(OperatorParser.constructOperatorForCache(state.getEventHolder(),
                    condition, matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext,
                    updateCachePolicyAttribute, this),
                    ExpressionParser.parseExpression(condition, matchingMetaInfoHolder.getMetaStateEvent(),
                            matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
                            false, 0, ProcessingMode.BATCH,
                            false, siddhiQueryContext),
                    matchingMetaInfoHolder.getStoreEventIndex()
            );
        } finally {
            stateHolder.returnState(state);
        }
    }

    public abstract void updateCachePolicyAttribute(StreamEvent streamEvent);

    /**
     * wrapper to send routeToCache bool with cache compiled condition
     */
    public class CacheCompiledConditionWithRouteToCache {
        private CompiledCondition cacheCompiledCondition;
        private boolean routeToCache;

        CacheCompiledConditionWithRouteToCache(CompiledCondition cacheCompiledCondition, boolean routeToCache) {
            this.cacheCompiledCondition = cacheCompiledCondition;
            this.routeToCache = routeToCache;
        }

        public CompiledCondition getCacheCompiledCondition() {
            return cacheCompiledCondition;
        }

        public boolean isRouteToCache() {
            return routeToCache;
        }
    }
}

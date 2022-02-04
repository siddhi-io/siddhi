/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.table.record;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.MetaStateEventAttribute;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.CacheTable;
import io.siddhi.core.table.CacheTableFIFO;
import io.siddhi.core.table.CacheTableLFU;
import io.siddhi.core.table.CacheTableLRU;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.cache.CacheExpirer;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.SelectorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.siddhi.core.util.OnDemandQueryRuntimeUtil.executeSelectorAndReturnStreamEvent;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_CACHE;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_CACHE_POLICY;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_CACHE_PURGE_INTERVAL;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_CACHE_RETENTION_PERIOD;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static io.siddhi.core.util.SiddhiConstants.CACHE_QUERY_NAME;
import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_SIZE;
import static io.siddhi.core.util.cache.CacheUtils.findEventChunkSize;
import static io.siddhi.core.util.parser.OnDemandQueryParser.buildExpectedOutputAttributes;
import static io.siddhi.core.util.parser.OnDemandQueryParser.generateMatchingMetaInfoHolderForCacheTable;
import static io.siddhi.query.api.util.AnnotationHelper.getAnnotation;

/**
 * An abstract implementation of table. Abstract implementation will handle {@link ComplexEventChunk} so that
 * developer can directly work with event data.
 */
public abstract class AbstractQueryableRecordTable extends AbstractRecordTable implements QueryableProcessor {
    private static final Logger log = LogManager.getLogger(AbstractQueryableRecordTable.class);
    public static ThreadLocal<Boolean> queryStoreWithoutCheckingCache = ThreadLocal.withInitial(() -> Boolean.FALSE);
    protected StateEvent findMatchingEvent;
    protected Selector selectorForTestOnDemandQuery;
    protected SiddhiQueryContext siddhiQueryContextForTestOnDemandQuery;
    protected MatchingMetaInfoHolder matchingMetaInfoHolderForTestOnDemandQuery;
    protected StateEvent containsMatchingEvent;
    private int maxCacheSize;
    private long purgeInterval;
    private boolean cacheEnabled = false;
    private boolean cacheExpiryEnabled = false;
    private InMemoryTable cacheTable;
    private CompiledCondition compiledConditionForCaching;
    private CompiledSelection compiledSelectionForCaching;
    private Attribute[] outputAttributesForCaching;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private long storeSizeLastCheckedTime;
    private long storeSizeCheckInterval;
    private long retentionPeriod;
    private long cacheLastReloadTime;
    private CompiledSelection compiledSelectionForSelectAll;
    private int storeTableSize = -1;

    @Override
    public void initCache(TableDefinition tableDefinition, SiddhiAppContext siddhiAppContext,
                          StreamEventCloner storeEventCloner, ConfigReader configReader) {
        String[] annotationNames = {ANNOTATION_STORE, ANNOTATION_CACHE};
        Annotation cacheTableAnnotation = getAnnotation(annotationNames, tableDefinition.getAnnotations());
        if (cacheTableAnnotation != null) {
            cacheEnabled = true;
            maxCacheSize = Integer.parseInt(cacheTableAnnotation.getElement(CACHE_TABLE_SIZE));
            TableDefinition cacheTableDefinition = TableDefinition.id(tableDefinition.getId());
            for (Attribute attribute : tableDefinition.getAttributeList()) {
                cacheTableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            for (Annotation annotation : tableDefinition.getAnnotations()) {
                if (!annotation.getName().equalsIgnoreCase("Store")) {
                    cacheTableDefinition.annotation(annotation);
                }
            }

            String cachePolicy = cacheTableAnnotation.getElement(ANNOTATION_CACHE_POLICY);

            if (cachePolicy == null || cachePolicy.equalsIgnoreCase("FIFO")) {
                cachePolicy = "FIFO";
                cacheTable = new CacheTableFIFO();
            } else if (cachePolicy.equalsIgnoreCase("LRU")) {
                cacheTable = new CacheTableLRU();
            } else if (cachePolicy.equalsIgnoreCase("LFU")) {
                cacheTable = new CacheTableLFU();
            } else {
                throw new SiddhiAppCreationException(siddhiAppContext.getName() + " : Cache policy can only be one " +
                        "of FIFO, LRU, and LFU but given as " + cachePolicy);
            }

            // check if cache expiry enabled and initialize relevant parameters
            if (cacheTableAnnotation.getElement(ANNOTATION_CACHE_RETENTION_PERIOD) != null) {
                cacheExpiryEnabled = true;
                retentionPeriod = Expression.Time.timeToLong(cacheTableAnnotation.
                        getElement(ANNOTATION_CACHE_RETENTION_PERIOD));
                if (cacheTableAnnotation.getElement(ANNOTATION_CACHE_PURGE_INTERVAL) == null) {
                    purgeInterval = retentionPeriod;
                } else {
                    purgeInterval = Expression.Time.timeToLong(cacheTableAnnotation.
                            getElement(ANNOTATION_CACHE_PURGE_INTERVAL));
                }
                storeSizeCheckInterval = purgeInterval * 5;
            } else {
                storeSizeCheckInterval = 10000;
            }

            ((CacheTable) cacheTable).initCacheTable(cacheTableDefinition, configReader, siddhiAppContext,
                    recordTableHandler, cacheExpiryEnabled, maxCacheSize, cachePolicy);

            // creating objects needed to load cache
            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext,
                    CACHE_QUERY_NAME + tableDefinition.getId());
            MatchingMetaInfoHolder matchingMetaInfoHolder =
                    generateMatchingMetaInfoHolderForCacheTable(tableDefinition);
            OnDemandQuery onDemandQuery = OnDemandQuery.query().
                    from(
                            InputStore.store(tableDefinition.getId())).
                    select(
                            Selector.selector().
                                    limit(Expression.value((maxCacheSize + 1)))
                    );
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();

            compiledConditionForCaching = compileCondition(Expression.value(true), matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
            List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(onDemandQuery,
                    tableMap, SiddhiConstants.UNKNOWN_STATE, matchingMetaInfoHolder, siddhiQueryContext);

            compiledSelectionForCaching = compileSelection(onDemandQuery.getSelector(), expectedOutputAttributes,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);

            outputAttributesForCaching = expectedOutputAttributes.toArray(new Attribute[0]);
            QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
            QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                    variableExpressionExecutors);
            compiledSelectionForSelectAll = generateCSForSelectAll();
        }
    }

    @Override
    protected void connectAndLoadCache() throws ConnectionUnavailableException {
        connect();
        if (cacheEnabled) {
            ((CacheTable) cacheTable).deleteAll();
            StateEvent stateEventForCaching = new StateEvent(1, 0);
            StreamEvent preLoadedData;
            queryStoreWithoutCheckingCache.set(Boolean.TRUE);
            try {
                preLoadedData = query(stateEventForCaching, compiledConditionForCaching,
                        compiledSelectionForCaching, outputAttributesForCaching);
            } finally {
                queryStoreWithoutCheckingCache.set(Boolean.FALSE);
            }

            if (preLoadedData != null) {
                ((CacheTable) cacheTable).addStreamEventUptoMaxSize(preLoadedData);
            }
            if (cacheExpiryEnabled) {
                siddhiAppContext.getScheduledExecutorService().scheduleAtFixedRate(
                        new CacheExpirer(retentionPeriod, cacheTable, tableMap, this, siddhiAppContext).
                                generateCacheExpirer(), 0, purgeInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection) throws ConnectionUnavailableException {
        return query(matchingEvent, compiledCondition, compiledSelection, null);
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        if (cacheEnabled) {
            readWriteLock.writeLock().lock();
            try {
                ((CacheTable) cacheTable).addAndTrimUptoMaxSize(addingEventChunk);
                super.add(addingEventChunk);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            super.add(addingEventChunk);
        }
    }

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition) {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        CompiledConditionWithCache compiledConditionWithCache;
        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            compiledConditionWithCache = (CompiledConditionWithCache)
                    (compiledConditionTemp.getCompiledCondition());
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());

            readWriteLock.writeLock().lock();
            try {
                cacheTable.delete(deletingEventChunk,
                        compiledConditionWithCache.getCacheCompileCondition());
                super.delete(deletingEventChunk, recordStoreCompiledCondition);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
            super.delete(deletingEventChunk, recordStoreCompiledCondition);
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet;
        CompiledConditionWithCache compiledConditionWithCache;
        CompiledUpdateSetWithCache compiledUpdateSetWithCache;
        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.getCompiledCondition();
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());
            compiledUpdateSetWithCache = (CompiledUpdateSetWithCache) compiledUpdateSet;
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet)
                    compiledUpdateSetWithCache.storeCompiledUpdateSet;
            readWriteLock.writeLock().lock();
            try {
                cacheTable.update(updatingEventChunk, compiledConditionWithCache.getCacheCompileCondition(),
                        compiledUpdateSetWithCache.getCacheCompiledUpdateSet());
                super.update(updatingEventChunk, recordStoreCompiledCondition, recordTableCompiledUpdateSet);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet) compiledUpdateSet;
            super.update(updatingEventChunk, recordStoreCompiledCondition, recordTableCompiledUpdateSet);
        }
    }

    @Override
    public boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        containsMatchingEvent = matchingEvent;
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        CompiledConditionWithCache compiledConditionWithCache;
        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.getCompiledCondition();
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());

            readWriteLock.readLock().lock();
            try {
                if (cacheTable.contains(matchingEvent, compiledConditionWithCache.getCacheCompileCondition())) {
                    return true;
                }
                return super.contains(matchingEvent, recordStoreCompiledCondition);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return super.contains(matchingEvent, compiledCondition);
        }
    }

    @Override
    public void updateOrAdd(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                            CompiledCondition compiledCondition, CompiledUpdateSet compiledUpdateSet,
                            AddingStreamEventExtractor addingStreamEventExtractor) {
        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.getCompiledCondition();
            RecordStoreCompiledCondition recordStoreCompiledCondition = new RecordStoreCompiledCondition(
                    compiledConditionTemp.variableExpressionExecutorMap,
                    compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());
            CompiledUpdateSetWithCache compiledUpdateSetWithCache = (CompiledUpdateSetWithCache) compiledUpdateSet;
            RecordTableCompiledUpdateSet recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet)
                    compiledUpdateSetWithCache.storeCompiledUpdateSet;

            readWriteLock.writeLock().lock();
            try {
                ((CacheTable) cacheTable).updateOrAddAndTrimUptoMaxSize(updateOrAddingEventChunk,
                        compiledConditionWithCache.getCacheCompileCondition(),
                        compiledUpdateSetWithCache.getCacheCompiledUpdateSet(), addingStreamEventExtractor,
                        maxCacheSize);
                super.updateOrAdd(updateOrAddingEventChunk, recordStoreCompiledCondition, recordTableCompiledUpdateSet,
                        addingStreamEventExtractor);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            super.updateOrAdd(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                    addingStreamEventExtractor);
        }
    }

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent)
            throws ConnectionUnavailableException {
        try {
            updateStoreTableSize();
        } catch (ConnectionUnavailableException e) {
            log.error(e);
        }

        // handle compile condition type conv
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        CompiledConditionWithCache compiledConditionWithCache = null;
        findMatchingEvent = matchingEvent;
        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.getCompiledCondition();
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());
        } else {
            recordStoreCompiledCondition =
                    ((RecordStoreCompiledCondition) compiledCondition);
        }

        StreamEvent cacheResults;
        if (cacheEnabled) {
            readWriteLock.writeLock().lock();
            try {
                // when table is smaller than max cache send results from cache
                if (storeTableSize <= maxCacheSize) {
                    if (log.isDebugEnabled()) {
                        log.debug(siddhiAppContext.getName() + "-" +
                                recordStoreCompiledCondition.getSiddhiQueryContext().getName() +
                                ": store table size is smaller than max cache. Sending results from cache");
                    }
                    cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                            matchingEvent);
                    return cacheResults;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(siddhiAppContext.getName() + "-" +
                                recordStoreCompiledCondition.getSiddhiQueryContext().getName() +
                                ": store table size is bigger than cache.");
                    }
                    if (compiledConditionWithCache.isRouteToCache()) {
                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" +
                                    recordStoreCompiledCondition.getSiddhiQueryContext().
                                            getName() + ": cache constraints satisfied. Checking cache");
                        }
                        cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                                matchingEvent);
                        if (cacheResults != null) {
                            if (log.isDebugEnabled()) {
                                log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.
                                        getSiddhiQueryContext().getName() + ": cache hit. Sending results from cache");
                            }
                            return cacheResults;
                        }
                        // cache miss
                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" +
                                    recordStoreCompiledCondition.getSiddhiQueryContext().
                                            getName() + ": cache miss. Loading from store");
                        }
                        StreamEvent streamEvent = super.find(recordStoreCompiledCondition, matchingEvent);
                        if (streamEvent == null) {
                            if (log.isDebugEnabled()) {
                                log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.
                                        getSiddhiQueryContext().getName() + ": store also miss. sending null");
                            }
                            return null;
                        }

                        if (cacheTable.size() == maxCacheSize) {
                            ((CacheTable) cacheTable).deleteOneEntryUsingCachePolicy();
                        }
                        ((CacheTable) cacheTable).addStreamEventUptoMaxSize(streamEvent);
                        cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                                matchingEvent);
                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.
                                    getSiddhiQueryContext().getName() +
                                    ": sending results from cache after loading from store");
                        }
                        return cacheResults;
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
        // when cache is not enabled or cache query conditions are not satisfied
        if (log.isDebugEnabled()) {
            log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.getSiddhiQueryContext().getName()
                    + ": sending results from store");
        }
        return super.find(recordStoreCompiledCondition, matchingEvent);
    }

    @Override
    public CompiledUpdateSet compileUpdateSet(UpdateSet updateSet,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        CompiledUpdateSet recordTableCompiledUpdateSet = super.compileUpdateSet(updateSet,
                matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
        if (cacheEnabled) {
            CompiledUpdateSet cacheCompileUpdateSet = cacheTable.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
            return new CompiledUpdateSetWithCache(recordTableCompiledUpdateSet, cacheCompileUpdateSet);
        }
        return recordTableCompiledUpdateSet;
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        ExpressionExecutor inMemoryCompiledCondition = ExpressionParser.parseExpression(condition,
                matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                variableExpressionExecutors, false, 0,
                ProcessingMode.BATCH, false, siddhiQueryContext);
        ExpressionBuilder expressionBuilder = new ExpressionBuilder(condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, new UpdateOrInsertReducer(
                inMemoryCompiledCondition, matchingMetaInfoHolder), null, siddhiQueryContext);
        CompiledCondition compileCondition = compileCondition(expressionBuilder);
        Map<String, ExpressionExecutor> expressionExecutorMap = expressionBuilder.getVariableExpressionExecutorMap();

        if (cacheEnabled) {
            CompiledCondition compiledConditionWithCache = new CompiledConditionWithCache(compileCondition,
                    ((CacheTable) cacheTable).generateCacheCompileCondition(condition, matchingMetaInfoHolder,
                            siddhiQueryContext, variableExpressionExecutors), siddhiQueryContext);
            return new RecordStoreCompiledCondition(expressionExecutorMap, compiledConditionWithCache,
                    siddhiQueryContext);
        } else {
            return new RecordStoreCompiledCondition(expressionExecutorMap, compileCondition, siddhiQueryContext);
        }
    }

    private void updateStoreTableSize() throws ConnectionUnavailableException {
        if (cacheEnabled && !queryStoreWithoutCheckingCache.get()) {
            readWriteLock.writeLock().lock();
            try {
                // check if we need to check the size of store
                if (storeTableSize == -1 || (!cacheExpiryEnabled && storeSizeLastCheckedTime < siddhiAppContext.
                        getTimestampGenerator().currentTime() - storeSizeCheckInterval)) {
                    StateEvent stateEventForCaching = new StateEvent(1, 0);
                    queryStoreWithoutCheckingCache.set(Boolean.TRUE);
                    try {
                        StreamEvent preLoadedData = query(stateEventForCaching, compiledConditionForCaching,
                                compiledSelectionForCaching, outputAttributesForCaching);
                        storeTableSize = findEventChunkSize(preLoadedData);
                        storeSizeLastCheckedTime = siddhiAppContext.getTimestampGenerator().currentTime();
                    } finally {
                        queryStoreWithoutCheckingCache.set(Boolean.FALSE);
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        findMatchingEvent = matchingEvent;
        updateStoreTableSize();

        // handle condition type convs
        ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>();
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        RecordStoreCompiledSelection recordStoreCompiledSelection;
        CompiledConditionWithCache compiledConditionWithCache = null;
        CompiledSelectionWithCache compiledSelectionWithCache = null;
        StreamEvent cacheResults;

        if (cacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.getCompiledCondition();
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(
                    compiledConditionTemp.variableExpressionExecutorMap,
                    compiledConditionWithCache.getStoreCompileCondition(),
                    compiledConditionTemp.getSiddhiQueryContext());

            compiledSelectionWithCache = (CompiledSelectionWithCache) compiledSelection;
            recordStoreCompiledSelection = compiledSelectionWithCache.recordStoreCompiledSelection;
        } else {
            recordStoreCompiledSelection = ((RecordStoreCompiledSelection) compiledSelection);
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
        }

        Map<String, Object> parameterMap = new HashMap<>();
        for (Map.Entry<String, ExpressionExecutor> entry :
                recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
            parameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }
        for (Map.Entry<String, ExpressionExecutor> entry :
                recordStoreCompiledSelection.variableExpressionExecutorMap.entrySet()) {
            parameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }

        Iterator<Object[]> records;
        if (cacheEnabled) {
            readWriteLock.writeLock().lock();
            try {
                // when store is smaller than max cache size
                if (storeTableSize <= maxCacheSize && !queryStoreWithoutCheckingCache.get()) {
                    // return results from cache
                    cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                            matchingEvent);
                    if (log.isDebugEnabled()) {
                        log.debug(siddhiAppContext.getName() + "-" +
                                recordStoreCompiledCondition.getSiddhiQueryContext().getName() +
                                ": store table size is smaller than max cache. Sending results from cache");
                    }
                    if (cacheResults == null) {
                        return null;
                    }
                    return executeSelectorOnCacheResults(compiledSelectionWithCache, cacheResults,
                            matchingEvent.getStreamEvent(0));
                } else { // when store is bigger than max cache size
                    if (log.isDebugEnabled() && !queryStoreWithoutCheckingCache.get()) {
                        log.debug(siddhiAppContext.getName() + "-" +
                                recordStoreCompiledCondition.getSiddhiQueryContext().getName() +
                                ": store table size is bigger than cache.");
                    }
                    if (compiledConditionWithCache.isRouteToCache() && !queryStoreWithoutCheckingCache.get()) {
                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" +
                                    recordStoreCompiledCondition.getSiddhiQueryContext().
                                            getName() + ": cache constraints satisfied. Checking cache");
                        }
                        // if query conrains all primary keys and has == only for them
                        cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                                matchingEvent);
                        if (cacheResults != null) {
                            if (log.isDebugEnabled()) {
                                log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.
                                        getSiddhiQueryContext().getName() + ": cache hit. Sending results from cache");
                            }
                            return executeSelectorOnCacheResults(compiledSelectionWithCache, cacheResults,
                                    matchingEvent.getStreamEvent(0));
                        }

                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" +
                                    recordStoreCompiledCondition.getSiddhiQueryContext().
                                            getName() + ": cache miss. Loading from store");
                        }
                        // read all fields of missed entry from store

                        Iterator<Object[]> recordsFromSelectAll;
                        if (recordTableHandler != null) {
                            recordsFromSelectAll = recordTableHandler.query(matchingEvent.getTimestamp(), parameterMap,
                                    recordStoreCompiledCondition.getCompiledCondition(), compiledSelectionForSelectAll,
                                    outputAttributes);
                        } else {
                            recordsFromSelectAll = query(parameterMap,
                                    recordStoreCompiledCondition.getCompiledCondition(),
                                    compiledSelectionForSelectAll, outputAttributes);
                        }
                        if (recordsFromSelectAll == null || !recordsFromSelectAll.hasNext()) {
                            if (log.isDebugEnabled()) {
                                log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.
                                        getSiddhiQueryContext().getName() + ": store also miss. sending null");
                            }
                            return null;
                        }
                        Object[] recordSelectAll = recordsFromSelectAll.next();
                        StreamEvent streamEvent = storeEventPool.newInstance();
                        streamEvent.setOutputData(new Object[outputAttributes.length]);
                        System.arraycopy(recordSelectAll, 0, streamEvent.getOutputData(), 0, recordSelectAll.length);

                        if (cacheTable.size() == maxCacheSize) {
                            ((CacheTable) cacheTable).deleteOneEntryUsingCachePolicy();
                        }
                        ((CacheTable) cacheTable).addStreamEventUptoMaxSize(streamEvent);
                        if (log.isDebugEnabled()) {
                            log.debug(siddhiAppContext.getName() + "-" +
                                    recordStoreCompiledCondition.getSiddhiQueryContext().getName() +
                                    ": sending results from cache after loading from store");
                        }
                        cacheResults = cacheTable.find(compiledConditionWithCache.getCacheCompileCondition(),
                                matchingEvent);
                        return executeSelectorOnCacheResults(compiledSelectionWithCache, cacheResults,
                                matchingEvent.getStreamEvent(0));
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
        if (log.isDebugEnabled() && !queryStoreWithoutCheckingCache.get()) {
            log.debug(siddhiAppContext.getName() + "-" + recordStoreCompiledCondition.getSiddhiQueryContext().
                    getName() + ": sending results from store");
        }
        // query conditions are not satisfied check from store/ cache not enabled
        if (recordTableHandler != null) {
            records = recordTableHandler.query(matchingEvent.getTimestamp(), parameterMap,
                    recordStoreCompiledCondition.getCompiledCondition(),
                    recordStoreCompiledSelection.compiledSelection, outputAttributes);
        } else {
            records = query(parameterMap, recordStoreCompiledCondition.getCompiledCondition(),
                    recordStoreCompiledSelection.compiledSelection, outputAttributes);
        }
        addStreamEventToChunk(outputAttributes, streamEventComplexEventChunk, records);
        return streamEventComplexEventChunk.getFirst();
    }

    private CompiledSelection generateCSForSelectAll() {
        MetaStreamEvent metaStreamEventForSelectAll = new MetaStreamEvent();
        for (Attribute attribute : tableDefinition.getAttributeList()) {
            metaStreamEventForSelectAll.addOutputData(attribute);
        }
        metaStreamEventForSelectAll.addInputDefinition(tableDefinition);
        MetaStateEvent metaStateEventForSelectAll = new MetaStateEvent(1);
        metaStateEventForSelectAll.addEvent(metaStreamEventForSelectAll);
        MatchingMetaInfoHolder matchingMetaInfoHolderForSlectAll = new
                MatchingMetaInfoHolder(metaStateEventForSelectAll, -1, 0, tableDefinition, tableDefinition, 0);

        List<OutputAttribute> outputAttributesAll = new ArrayList<>();
        List<Attribute> attributeList = tableDefinition.getAttributeList();
        for (Attribute attribute : attributeList) {
            outputAttributesAll.add(new OutputAttribute(new Variable(attribute.getName())));
        }

        List<SelectAttributeBuilder> selectAttributeBuilders = new ArrayList<>(outputAttributesAll.size());
        List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
        for (OutputAttribute outputAttribute : outputAttributesAll) {
            ExpressionBuilder expressionBuilder = new ExpressionBuilder(outputAttribute.getExpression(),
                    matchingMetaInfoHolderForSlectAll, variableExpressionExecutors, tableMap,
                    null, null, null);
            selectAttributeBuilders.add(new SelectAttributeBuilder(expressionBuilder,
                    outputAttribute.getRename()));
        }
        return compileSelection(selectAttributeBuilders, null, null, null, null, null);
    }

    private void addStreamEventToChunk(Attribute[] outputAttributes, ComplexEventChunk<StreamEvent>
            streamEventComplexEventChunk, Iterator<Object[]> records) {
        if (records != null) {
            while (records.hasNext()) {
                Object[] record = records.next();
                StreamEvent streamEvent = storeEventPool.newInstance();
                streamEvent.setOutputData(new Object[outputAttributes.length]);
                System.arraycopy(record, 0, streamEvent.getOutputData(), 0, record.length);
                streamEventComplexEventChunk.add(streamEvent);
            }
        }
    }

    private StreamEvent executeSelectorOnCacheResults(CompiledSelectionWithCache compiledSelectionWithCache,
                                                      StreamEvent cacheResults, StreamEvent streamEvent) {
        StateEventFactory stateEventFactory = new StateEventFactory(compiledSelectionWithCache.metaStateEvent);
        return executeSelectorAndReturnStreamEvent(stateEventFactory, streamEvent, cacheResults,
                compiledSelectionWithCache.getStoreEventIndex(), compiledSelectionWithCache.querySelector);
    }

    /**
     * Query records matching the compiled condition and selection
     *
     * @param parameterMap      map of matching StreamVariable Ids and their values
     *                          corresponding to the compiled condition and selection
     * @param compiledCondition the compiledCondition against which records should be matched
     * @param compiledSelection the compiledSelection that maps records based to requested format
     * @return RecordIterator of matching records
     * @throws ConnectionUnavailableException
     */
    protected abstract RecordIterator<Object[]> query(Map<String, Object> parameterMap,
                                                      CompiledCondition compiledCondition,
                                                      CompiledSelection compiledSelection,
                                                      Attribute[] outputAttributes)
            throws ConnectionUnavailableException;

    public CompiledSelection compileSelection(Selector selector,
                                              List<Attribute> expectedOutputAttributes,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        selectorForTestOnDemandQuery = selector;
        siddhiQueryContextForTestOnDemandQuery = siddhiQueryContext;
        matchingMetaInfoHolderForTestOnDemandQuery = matchingMetaInfoHolder;
        List<OutputAttribute> outputAttributes = selector.getSelectionList();
        if (outputAttributes.size() == 0) {
            MetaStreamEvent metaStreamEvent = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(
                    matchingMetaInfoHolder.getStoreEventIndex());
            List<Attribute> attributeList = metaStreamEvent.getLastInputDefinition().getAttributeList();
            for (Attribute attribute : attributeList) {
                outputAttributes.add(new OutputAttribute(new Variable(attribute.getName())));
            }
        }
        List<SelectAttributeBuilder> selectAttributeBuilders = new ArrayList<>(outputAttributes.size());
        for (OutputAttribute outputAttribute : outputAttributes) {
            ExpressionBuilder expressionBuilder = new ExpressionBuilder(outputAttribute.getExpression(),
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap,
                    null, null, siddhiQueryContext);
            selectAttributeBuilders.add(new SelectAttributeBuilder(expressionBuilder, outputAttribute.getRename()));
        }

        MatchingMetaInfoHolder metaInfoHolderAfterSelect = new MatchingMetaInfoHolder(
                matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getMatchingStreamEventIndex(),
                matchingMetaInfoHolder.getStoreEventIndex(), matchingMetaInfoHolder.getMatchingStreamDefinition(),
                matchingMetaInfoHolder.getMatchingStreamDefinition(), matchingMetaInfoHolder.getCurrentState());

        List<ExpressionBuilder> groupByExpressionBuilders = null;
        if (selector.getGroupByList().size() != 0) {
            groupByExpressionBuilders = new ArrayList<>(outputAttributes.size());
            for (Variable variable : selector.getGroupByList()) {
                groupByExpressionBuilders.add(new ExpressionBuilder(variable, metaInfoHolderAfterSelect,
                        variableExpressionExecutors, tableMap, null, null, siddhiQueryContext));
            }
        }

        ExpressionBuilder havingExpressionBuilder = null;
        if (selector.getHavingExpression() != null) {
            havingExpressionBuilder = new ExpressionBuilder(selector.getHavingExpression(), metaInfoHolderAfterSelect,
                    variableExpressionExecutors, tableMap, null, null, siddhiQueryContext);
        }

        List<OrderByAttributeBuilder> orderByAttributeBuilders = null;
        if (selector.getOrderByList().size() != 0) {
            orderByAttributeBuilders = new ArrayList<>(selector.getOrderByList().size());
            for (OrderByAttribute orderByAttribute : selector.getOrderByList()) {
                ExpressionBuilder expressionBuilder = new ExpressionBuilder(orderByAttribute.getVariable(),
                        metaInfoHolderAfterSelect, variableExpressionExecutors,
                        tableMap, null, null, siddhiQueryContext);
                orderByAttributeBuilders.add(new OrderByAttributeBuilder(expressionBuilder,
                        orderByAttribute.getOrder()));
            }
        }

        Long limit = null;
        Long offset = null;
        if (selector.getLimit() != null) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression((Expression) selector.getLimit(),
                    metaInfoHolderAfterSelect.getMetaStateEvent(), SiddhiConstants.HAVING_STATE, tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            limit = ((Number) (((ConstantExpressionExecutor) expressionExecutor).getValue())).longValue();
            if (limit < 0) {
                throw new SiddhiAppCreationException("'limit' cannot have negative value, but found '" + limit + "'",
                        selector, siddhiQueryContext.getSiddhiAppContext());
            }
        }
        if (selector.getOffset() != null) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression((Expression) selector.getOffset(),
                    metaInfoHolderAfterSelect.getMetaStateEvent(), SiddhiConstants.HAVING_STATE, tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            offset = ((Number) (((ConstantExpressionExecutor) expressionExecutor).getValue())).longValue();
            if (offset < 0) {
                throw new SiddhiAppCreationException("'offset' cannot have negative value, but found '" + offset + "'",
                        selector, siddhiQueryContext.getSiddhiAppContext());
            }
        }
        CompiledSelection compiledSelection = compileSelection(selectAttributeBuilders, groupByExpressionBuilders,
                havingExpressionBuilder, orderByAttributeBuilders, limit, offset);

        Map<String, ExpressionExecutor> expressionExecutorMap = new HashMap<>();
        if (selectAttributeBuilders.size() != 0) {
            for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
                expressionExecutorMap.putAll(
                        selectAttributeBuilder.getExpressionBuilder().getVariableExpressionExecutorMap());
            }
        }
        if (groupByExpressionBuilders != null && groupByExpressionBuilders.size() != 0) {
            for (ExpressionBuilder groupByExpressionBuilder : groupByExpressionBuilders) {
                expressionExecutorMap.putAll(groupByExpressionBuilder.getVariableExpressionExecutorMap());
            }
        }
        if (havingExpressionBuilder != null) {
            expressionExecutorMap.putAll(havingExpressionBuilder.getVariableExpressionExecutorMap());
        }
        if (orderByAttributeBuilders != null && orderByAttributeBuilders.size() != 0) {
            for (OrderByAttributeBuilder orderByAttributeBuilder : orderByAttributeBuilders) {
                expressionExecutorMap.putAll(
                        orderByAttributeBuilder.getExpressionBuilder().getVariableExpressionExecutorMap());
            }
        }

        if (cacheEnabled) {
            CompiledSelectionWithCache compiledSelectionWithCache;
            MetaStateEvent metaStateEventForCacheSelection = matchingMetaInfoHolder.getMetaStateEvent().clone();
            ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
            int metaPosition = SiddhiConstants.UNKNOWN_STATE;
            List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector = new ArrayList<>();
            QuerySelector querySelector = SelectorParser.parse(selector,
                    returnStream,
                    metaStateEventForCacheSelection, tableMap, variableExpressionExecutorsForQuerySelector,
                    metaPosition, ProcessingMode.BATCH, false, siddhiQueryContext);
            if (matchingMetaInfoHolder.getMetaStateEvent().getOutputDataAttributes().size() == 0) {
                for (MetaStateEventAttribute outputDataAttribute : metaStateEventForCacheSelection.
                        getOutputDataAttributes()) {
                    matchingMetaInfoHolder.getMetaStateEvent().addOutputDataAllowingDuplicate(outputDataAttribute);
                }
            }
            QueryParserHelper.updateVariablePosition(metaStateEventForCacheSelection,
                    variableExpressionExecutorsForQuerySelector);
            querySelector.setEventPopulator(
                    StateEventPopulatorFactory.constructEventPopulator(metaStateEventForCacheSelection));

            RecordStoreCompiledSelection recordStoreCompiledSelection =
                    new RecordStoreCompiledSelection(expressionExecutorMap, compiledSelection);

            compiledSelectionWithCache = new CompiledSelectionWithCache(recordStoreCompiledSelection, querySelector,
                    metaStateEventForCacheSelection, matchingMetaInfoHolder.getStoreEventIndex(),
                    variableExpressionExecutorsForQuerySelector);
            return compiledSelectionWithCache;
        } else {
            return new RecordStoreCompiledSelection(expressionExecutorMap, compiledSelection);
        }
    }

    /**
     * Compile the query selection
     *
     * @param selectAttributeBuilders  helps visiting the select attributes in order
     * @param groupByExpressionBuilder helps visiting the group by attributes in order
     * @param havingExpressionBuilder  helps visiting the having condition
     * @param orderByAttributeBuilders helps visiting the order by attributes in order
     * @param limit                    defines the limit level
     * @param offset                   defines the offset level
     * @return compiled selection that can be used for retrieving events on a defined format
     */
    protected abstract CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                          List<ExpressionBuilder> groupByExpressionBuilder,
                                                          ExpressionBuilder havingExpressionBuilder,
                                                          List<OrderByAttributeBuilder> orderByAttributeBuilders,
                                                          Long limit, Long offset);

    public void handleCacheExpiry(CompiledCondition cacheExpiryCompiledCondition,
                                  ComplexEventChunk<StateEvent> deleteEventChunk) {
        if (log.isDebugEnabled()) {
            log.debug(siddhiAppContext.getName() + ": CacheExpirer started");
        }
        StateEvent stateEventForCaching = new StateEvent(1, 0);
        StreamEvent loadedDataFromStore;

        readWriteLock.writeLock().lock();
        try {
            if (storeTableSize != -1 && storeSizeLastCheckedTime >
                    siddhiAppContext.getTimestampGenerator().currentTime() - retentionPeriod * 10) {
                if (log.isDebugEnabled()) {
                    log.debug(siddhiAppContext.getName() + ": checking size of store table");
                }
                try {
                    if (storeTableSize <= maxCacheSize) {
                        AbstractQueryableRecordTable.queryStoreWithoutCheckingCache.set(Boolean.TRUE);
                        try {
                            if (cacheLastReloadTime < siddhiAppContext.getTimestampGenerator().currentTime() +
                                    retentionPeriod) {
                                loadedDataFromStore = query(stateEventForCaching, compiledConditionForCaching,
                                        compiledSelectionForCaching, outputAttributesForCaching);
                                clearCacheAndReload(loadedDataFromStore);
                                cacheLastReloadTime = siddhiAppContext.getTimestampGenerator().currentTime();
                            }
                        } finally {
                            AbstractQueryableRecordTable.queryStoreWithoutCheckingCache.set(Boolean.FALSE);
                        }
                    } else {
                        cacheTable.delete(deleteEventChunk, cacheExpiryCompiledCondition);
                    }
                } catch (ConnectionUnavailableException e) {
                    throw new SiddhiAppRuntimeException(siddhiAppContext.getName() + ": " + e.getMessage());
                }
            } else {

                try {
                    AbstractQueryableRecordTable.queryStoreWithoutCheckingCache.set(Boolean.TRUE);
                    try {
                        loadedDataFromStore = query(stateEventForCaching, compiledConditionForCaching,
                                compiledSelectionForCaching, outputAttributesForCaching);
                        storeTableSize = findEventChunkSize(loadedDataFromStore);
                        storeSizeLastCheckedTime = siddhiAppContext.getTimestampGenerator().currentTime();
                    } finally {
                        AbstractQueryableRecordTable.queryStoreWithoutCheckingCache.set(Boolean.FALSE);
                    }
                    if (storeTableSize <= maxCacheSize) {
                        if (cacheLastReloadTime < siddhiAppContext.getTimestampGenerator().currentTime() +
                                retentionPeriod) {
                            clearCacheAndReload(loadedDataFromStore);
                            cacheLastReloadTime = siddhiAppContext.getTimestampGenerator().currentTime();
                        }
                    } else {
                        cacheTable.delete(deleteEventChunk, cacheExpiryCompiledCondition);
                    }
                } catch (Exception e) {
                    throw new SiddhiAppRuntimeException(siddhiAppContext.getName() + ": " + e.getMessage());
                }
            }
            if (log.isDebugEnabled()) {
                log.debug(siddhiAppContext.getName() + ": CacheExpirer ended");
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void clearCacheAndReload(StreamEvent loadedDataFromStore) {
        ((CacheTable) cacheTable).deleteAll();
        if (loadedDataFromStore != null) {
            ((CacheTable) cacheTable).addStreamEventUptoMaxSize(loadedDataFromStore);
        }
    }

    /**
     * Class to wrap store compile update set and cache compile update set
     */
    private class CompiledUpdateSetWithCache implements CompiledUpdateSet {
        CompiledUpdateSet storeCompiledUpdateSet;
        CompiledUpdateSet cacheCompiledUpdateSet;

        public CompiledUpdateSetWithCache(CompiledUpdateSet storeCompiledUpdateSet,
                                          CompiledUpdateSet cacheCompiledUpdateSet) {
            this.storeCompiledUpdateSet = storeCompiledUpdateSet;
            this.cacheCompiledUpdateSet = cacheCompiledUpdateSet;
        }

        public CompiledUpdateSet getStoreCompiledUpdateSet() {
            return storeCompiledUpdateSet;
        }

        public CompiledUpdateSet getCacheCompiledUpdateSet() {
            return cacheCompiledUpdateSet;
        }
    }

    /**
     * Class to hold store compile condition and cache compile condition wrapped
     */
    private class CompiledConditionWithCache implements CompiledCondition {
        private CompiledCondition cacheCompileCondition;
        private CompiledCondition storeCompileCondition;
        private boolean routeToCache;
        private SiddhiQueryContext siddhiQueryContext;

        CompiledConditionWithCache(CompiledCondition storeCompileCondition,
                                   CacheTable.CacheCompiledConditionWithRouteToCache
                                           cacheCompiledConditionWithRouteToCache,
                                   SiddhiQueryContext siddhiQueryContext) {
            this.storeCompileCondition = storeCompileCondition;
            this.cacheCompileCondition = cacheCompiledConditionWithRouteToCache.getCacheCompiledCondition();
            this.routeToCache = cacheCompiledConditionWithRouteToCache.isRouteToCache();
            this.siddhiQueryContext = siddhiQueryContext;
        }

        CompiledCondition getStoreCompileCondition() {
            return storeCompileCondition;
        }

        boolean isRouteToCache() {
            return routeToCache;
        }

        CompiledCondition getCacheCompileCondition() {
            return cacheCompileCondition;
        }

        public SiddhiQueryContext getSiddhiQueryContext() {
            return siddhiQueryContext;
        }
    }

    /**
     * class to hold both store compile selection and cache compile selection wrapped
     */
    public class CompiledSelectionWithCache implements CompiledSelection {
        private QuerySelector querySelector;
        private MetaStateEvent metaStateEvent;
        private RecordStoreCompiledSelection recordStoreCompiledSelection;
        private int storeEventIndex;
        private List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector;

        public CompiledSelectionWithCache(RecordStoreCompiledSelection recordStoreCompiledSelection,
                                          QuerySelector querySelector, MetaStateEvent metaStateEvent,
                                          int storeEventIndex,
                                          List<VariableExpressionExecutor> variableExpressionExecutors) {
            this.recordStoreCompiledSelection = recordStoreCompiledSelection;
            this.querySelector = querySelector;
            this.metaStateEvent = metaStateEvent;
            this.storeEventIndex = storeEventIndex;
            this.variableExpressionExecutorsForQuerySelector = variableExpressionExecutors;
        }

        public RecordStoreCompiledSelection getRecordStoreCompiledSelection() {
            return recordStoreCompiledSelection;
        }

        public QuerySelector getQuerySelector() {
            return querySelector;
        }

        public MetaStateEvent getMetaStateEvent() {
            return metaStateEvent;
        }

        public int getStoreEventIndex() {
            return storeEventIndex;
        }

        public List<VariableExpressionExecutor> getVariableExpressionExecutorsForQuerySelector() {
            return variableExpressionExecutorsForQuerySelector;
        }
    }

    private class RecordStoreCompiledSelection implements CompiledSelection {
        private final Map<String, ExpressionExecutor> variableExpressionExecutorMap;
        private final CompiledSelection compiledSelection;

        RecordStoreCompiledSelection(Map<String, ExpressionExecutor> variableExpressionExecutorMap,
                                     CompiledSelection compiledSelection) {

            this.variableExpressionExecutorMap = variableExpressionExecutorMap;
            this.compiledSelection = compiledSelection;
        }
    }

    /**
     * Holder of Selection attribute with renaming field
     */
    public class SelectAttributeBuilder {
        private final ExpressionBuilder expressionBuilder;
        private final String rename;

        public SelectAttributeBuilder(ExpressionBuilder expressionBuilder, String rename) {
            this.expressionBuilder = expressionBuilder;
            this.rename = rename;
        }

        public ExpressionBuilder getExpressionBuilder() {
            return expressionBuilder;
        }

        public String getRename() {
            return rename;
        }
    }

    /**
     * Holder of order by attribute with order orientation
     */
    public class OrderByAttributeBuilder {
        private final ExpressionBuilder expressionBuilder;
        private final OrderByAttribute.Order order;

        public OrderByAttributeBuilder(ExpressionBuilder expressionBuilder, OrderByAttribute.Order order) {
            this.expressionBuilder = expressionBuilder;
            this.order = order;
        }

        public ExpressionBuilder getExpressionBuilder() {
            return expressionBuilder;
        }

        public OrderByAttribute.Order getOrder() {
            return order;
        }
    }
}

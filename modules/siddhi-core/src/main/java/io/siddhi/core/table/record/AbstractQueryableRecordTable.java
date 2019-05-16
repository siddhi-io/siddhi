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
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.SelectorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.siddhi.core.util.CacheUtils.findEventChunkSize;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_CACHE;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static io.siddhi.core.util.SiddhiConstants.CACHE_QUERY_NAME;
import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_SIZE;
import static io.siddhi.core.util.StoreQueryRuntimeUtil.executeSelector;
import static io.siddhi.core.util.parser.StoreQueryParser.buildExpectedOutputAttributes;
import static io.siddhi.core.util.parser.StoreQueryParser.generateMatchingMetaInfoHolderForCacheTable;
import static io.siddhi.query.api.util.AnnotationHelper.getNestedAnnotation;

/**
 * An abstract implementation of table. Abstract implementation will handle {@link ComplexEventChunk} so that
 * developer can directly work with event data.
 */
public abstract class AbstractQueryableRecordTable extends AbstractRecordTable implements QueryableProcessor {
    private static final Logger log = Logger.getLogger(AbstractQueryableRecordTable.class);
    private int maxCacheSize;
    private InMemoryTable cachedTable;
    private boolean isCacheEnabled = false;
    private CompiledCondition compiledConditionForCaching;
    private CompiledSelection compiledSelectionForCaching;
    private Attribute[] outputAttributesForCaching;
    private TableDefinition cacheTableDefinition;
    private SiddhiAppContext siddhiAppContext;

    @Override
    public void init(TableDefinition tableDefinition, SiddhiAppContext siddhiAppContext,
                     StreamEventCloner storeEventCloner, ConfigReader configReader) {
        this.siddhiAppContext = siddhiAppContext;
        String[] annotationNames = {ANNOTATION_STORE, ANNOTATION_CACHE};
        Annotation cacheTableAnnotation = getNestedAnnotation(tableDefinition.getAnnotations(), annotationNames);
        if (cacheTableAnnotation != null) {
            isCacheEnabled = true;
            maxCacheSize = Integer.parseInt(cacheTableAnnotation.getElement(CACHE_TABLE_SIZE));
            cachedTable = new InMemoryTable();
            cacheTableDefinition = new TableDefinition(tableDefinition.getId());
            for (Attribute attribute: tableDefinition.getAttributeList()) {
                cacheTableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            for (Annotation annotation: tableDefinition.getAnnotations()) {
                if (!annotation.getName().equalsIgnoreCase("Store")) {
                    cacheTableDefinition.annotation(annotation);
                }
            }
            cachedTable.initTable(cacheTableDefinition, storeEventPool,
                    storeEventCloner, configReader, siddhiAppContext, recordTableHandler);

            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext,
                    CACHE_QUERY_NAME + cacheTableDefinition.getId());
            MatchingMetaInfoHolder matchingMetaInfoHolder =
                    generateMatchingMetaInfoHolderForCacheTable(cacheTableDefinition);
            StoreQuery storeQuery = StoreQuery.query().
                    from(
                            InputStore.store(cacheTableDefinition.getId())).
                    select(
                            Selector.selector().
                                    limit(Expression.value((maxCacheSize + 1)))
                    );
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();

            compiledConditionForCaching = compileCondition(Expression.value(true), matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
            List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(storeQuery,
                    tableMap, SiddhiConstants.UNKNOWN_STATE, matchingMetaInfoHolder, siddhiQueryContext);
            compiledSelectionForCaching = compileSelection(storeQuery.getSelector(), expectedOutputAttributes,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
            outputAttributesForCaching = expectedOutputAttributes.toArray(new Attribute[0]);
            QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
            QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                    variableExpressionExecutors);
        }
    }

    @Override
    protected void connectAndLoadCache() throws ConnectionUnavailableException {
        connect();
        if (isCacheEnabled) {
            StateEvent stateEventForCaching = new StateEvent(1, 0);
            StreamEvent preLoadedData = query(stateEventForCaching, compiledConditionForCaching,
                    compiledSelectionForCaching, outputAttributesForCaching);

            if (preLoadedData != null) {
                int preLoadedDataSize = findEventChunkSize(preLoadedData);
                if (preLoadedDataSize <= maxCacheSize) {
                    ComplexEventChunk<StreamEvent> loadedCache = new ComplexEventChunk<>();
                    loadedCache.add(preLoadedData);
                    cachedTable.addEvents(loadedCache, preLoadedDataSize);
                } else {
                    isCacheEnabled = false;
                    log.warn(siddhiAppContext.getName() + ": " + cacheTableDefinition.getId() + " size is bigger " +
                            "than cache table size defined as " + maxCacheSize + ". So cache is disabled");
                }
            }
        }
    }

    private CompiledCondition generateCacheCompileCondition(Expression condition,
                                                            MatchingMetaInfoHolder storeMatchingMetaInfoHolder,
                                                            SiddhiQueryContext siddhiQueryContext,
                                                            List<VariableExpressionExecutor>
                                                                    storeVariableExpressionExecutors) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents().length);
        for (MetaStreamEvent referenceMetaStreamEvent: storeMatchingMetaInfoHolder.getMetaStateEvent().
                getMetaStreamEvents()) {
            metaStateEvent.addEvent(referenceMetaStreamEvent);
        }
        MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(
                metaStateEvent,
                storeMatchingMetaInfoHolder.getMatchingStreamEventIndex(),
                storeMatchingMetaInfoHolder.getStoreEventIndex(),
                storeMatchingMetaInfoHolder.getMatchingStreamDefinition(),
                cacheTableDefinition,
                storeMatchingMetaInfoHolder.getCurrentState());

        Map<String, Table> tableMap = new ConcurrentHashMap<>();
        tableMap.put(cacheTableDefinition.getId(), cachedTable);

        return cachedTable.compileCondition(condition, matchingMetaInfoHolder,
                storeVariableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    protected abstract void connect() throws ConnectionUnavailableException;

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection) throws ConnectionUnavailableException {
        return query(matchingEvent, compiledCondition, compiledSelection, null);
    }

    public StreamEvent findInCache(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        if (isCacheEnabled) {
            return cachedTable.find(matchingEvent, compiledCondition);
        } else {
            return null;
        }
    }

    private Object[] appendValue(Object[] obj, Object newObj) {

        ArrayList<Object> temp = new ArrayList<Object>(Arrays.asList(obj));
        temp.add(newObj);
        return temp.toArray();

    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) throws ConnectionUnavailableException {
        if (isCacheEnabled) {
            ComplexEventChunk<StreamEvent> addingEventChunkWithTimestamp = new ComplexEventChunk<>();
            while (addingEventChunk.hasNext()) {
                StreamEvent event = addingEventChunk.next();
                Object[] outputData = event.getOutputData();
                Object[] outputDataWithTimeStamp = appendValue(outputData,
                        new Timestamp(System.currentTimeMillis()).getTime());
                StreamEvent eventWithTimeStamp = new StreamEvent(0, 0, outputDataWithTimeStamp.length);
                eventWithTimeStamp.setOutputData(outputDataWithTimeStamp);
                addingEventChunkWithTimestamp.add(eventWithTimeStamp);
            }
            cachedTable.add(addingEventChunk);
            if (cachedTable.size() > maxCacheSize) {
                isCacheEnabled = false;
                log.warn(siddhiAppContext.getName() + ": " + cacheTableDefinition.getId() + " size is now " +
                        cachedTable.size() + " which is bigger than cache table size defined as " + maxCacheSize +
                        ". So cache is now disabled");
            }
        }
        List<Object[]> records = new ArrayList<>();
        addingEventChunk.reset();
        long timestamp = 0L;
        while (addingEventChunk.hasNext()) {
            StreamEvent event = addingEventChunk.next();
            records.add(event.getOutputData());
            timestamp = event.getTimestamp();
        }
        if (recordTableHandler != null) {
            recordTableHandler.add(timestamp, records);
        } else {
            add(records);
        }
    }

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        if (isCacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition());
            cachedTable.delete(deletingEventChunk,
                    compiledConditionWithCache.getCacheCompileCondition());
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
        }
        List<Map<String, Object>> deleteConditionParameterMaps = new ArrayList<>();
        deletingEventChunk.reset();
        long timestamp = 0L;
        while (deletingEventChunk.hasNext()) {
            StateEvent stateEvent = deletingEventChunk.next();

            Map<String, Object> variableMap = new HashMap<>();
            for (Map.Entry<String, ExpressionExecutor> entry :
                    recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
                variableMap.put(entry.getKey(), entry.getValue().execute(stateEvent));
            }

            deleteConditionParameterMaps.add(variableMap);
            timestamp = stateEvent.getTimestamp();
        }
        if (recordTableHandler != null) {
            recordTableHandler.delete(timestamp, deleteConditionParameterMaps, recordStoreCompiledCondition.
                    compiledCondition);
        } else {
            delete(deleteConditionParameterMaps, recordStoreCompiledCondition.compiledCondition);
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet;
        if (isCacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition());
            CompiledUpdateSetWithCache compiledUpdateSetWithCache = (CompiledUpdateSetWithCache) compiledUpdateSet;
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet)
                    compiledUpdateSetWithCache.storeCompiledUpdateSet;
            cachedTable.update(updatingEventChunk, compiledConditionWithCache.getCacheCompileCondition(),
                    compiledUpdateSetWithCache.getCacheCompiledUpdateSet());
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet) compiledUpdateSet;
        }
        List<Map<String, Object>> updateConditionParameterMaps = new ArrayList<>();
        List<Map<String, Object>> updateSetParameterMaps = new ArrayList<>();
        updatingEventChunk.reset();
        long timestamp = 0L;
        while (updatingEventChunk.hasNext()) {
            StateEvent stateEvent = updatingEventChunk.next();

            Map<String, Object> variableMap = new HashMap<>();
            for (Map.Entry<String, ExpressionExecutor> entry :
                    recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
                variableMap.put(entry.getKey(), entry.getValue().execute(stateEvent));
            }
            updateConditionParameterMaps.add(variableMap);

            Map<String, Object> variableMapForUpdateSet = new HashMap<>();
            for (Map.Entry<String, ExpressionExecutor> entry :
                    recordTableCompiledUpdateSet.getExpressionExecutorMap().entrySet()) {
                variableMapForUpdateSet.put(entry.getKey(), entry.getValue().execute(stateEvent));
            }
            updateSetParameterMaps.add(variableMapForUpdateSet);
            timestamp = stateEvent.getTimestamp();
        }
        if (recordTableHandler != null) {
            recordTableHandler.update(timestamp, recordStoreCompiledCondition.compiledCondition,
                    updateConditionParameterMaps, recordTableCompiledUpdateSet.getUpdateSetMap(),
                    updateSetParameterMaps);
        } else {
            update(recordStoreCompiledCondition.compiledCondition, updateConditionParameterMaps,
                    recordTableCompiledUpdateSet.getUpdateSetMap(), updateSetParameterMaps);
        }
    }

    @Override
    public boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        if (isCacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition());
            if (cachedTable.contains(matchingEvent, compiledConditionWithCache.getCacheCompileCondition())) {
                return true;
            }
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
        }
        Map<String, Object> containsConditionParameterMap = new HashMap<>();
        for (Map.Entry<String, ExpressionExecutor> entry :
                recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
            containsConditionParameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }
        if (recordTableHandler != null) {
            return recordTableHandler.contains(matchingEvent.getTimestamp(), containsConditionParameterMap,
                    recordStoreCompiledCondition.compiledCondition);
        } else {
            return contains(containsConditionParameterMap, recordStoreCompiledCondition.compiledCondition);
        }
    }

    @Override
    public void updateOrAdd(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                            CompiledCondition compiledCondition, CompiledUpdateSet compiledUpdateSet,
                            AddingStreamEventExtractor addingStreamEventExtractor)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet;
        if (isCacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionWithCache.getStoreCompileCondition());
            CompiledUpdateSetWithCache compiledUpdateSetWithCache = (CompiledUpdateSetWithCache) compiledUpdateSet;
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet)
                    compiledUpdateSetWithCache.storeCompiledUpdateSet;
            cachedTable.updateOrAdd(updateOrAddingEventChunk,
                    compiledConditionWithCache.getCacheCompileCondition(),
                    compiledUpdateSetWithCache.getCacheCompiledUpdateSet(), addingStreamEventExtractor);
            if (cachedTable.size() > maxCacheSize) {
                isCacheEnabled = false;
                log.warn(siddhiAppContext.getName() + ": " + cacheTableDefinition.getId() + " size is now " +
                        cachedTable.size() + " which is bigger than cache table size defined as " + maxCacheSize +
                        ". So cache is now disabled");
            }
        } else {
            recordStoreCompiledCondition = ((RecordStoreCompiledCondition) compiledCondition);
            recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet) compiledUpdateSet;
        }
        List<Map<String, Object>> updateConditionParameterMaps = new ArrayList<>();
        List<Map<String, Object>> updateSetParameterMaps = new ArrayList<>();
        List<Object[]> addingRecords = new ArrayList<>();
        updateOrAddingEventChunk.reset();
        long timestamp = 0L;
        while (updateOrAddingEventChunk.hasNext()) {
            StateEvent stateEvent = updateOrAddingEventChunk.next();

            Map<String, Object> variableMap = new HashMap<>();
            for (Map.Entry<String, ExpressionExecutor> entry :
                    recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
                variableMap.put(entry.getKey(), entry.getValue().execute(stateEvent));
            }
            updateConditionParameterMaps.add(variableMap);

            Map<String, Object> variableMapForUpdateSet = new HashMap<>();
            for (Map.Entry<String, ExpressionExecutor> entry :
                    recordTableCompiledUpdateSet.getExpressionExecutorMap().entrySet()) {
                variableMapForUpdateSet.put(entry.getKey(), entry.getValue().execute(stateEvent));
            }
            updateSetParameterMaps.add(variableMapForUpdateSet);
            addingRecords.add(stateEvent.getStreamEvent(0).getOutputData());
            timestamp = stateEvent.getTimestamp();
        }
        if (recordTableHandler != null) {
            recordTableHandler.updateOrAdd(timestamp, recordStoreCompiledCondition.compiledCondition,
                    updateConditionParameterMaps, recordTableCompiledUpdateSet.getUpdateSetMap(),
                    updateSetParameterMaps, addingRecords);
        } else {
            updateOrAdd(recordStoreCompiledCondition.compiledCondition, updateConditionParameterMaps,
                    recordTableCompiledUpdateSet.getUpdateSetMap(), updateSetParameterMaps, addingRecords);
        }

    }

    @Override
    public CompiledUpdateSet compileUpdateSet(UpdateSet updateSet,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet = new RecordTableCompiledUpdateSet();
        Map<String, ExpressionExecutor> parentExecutorMap = new HashMap<>();
        for (UpdateSet.SetAttribute setAttribute : updateSet.getSetAttributeList()) {
            ExpressionBuilder expressionBuilder = new ExpressionBuilder(setAttribute.getAssignmentExpression(),
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
            CompiledExpression compiledExpression = compileSetAttribute(expressionBuilder);
            recordTableCompiledUpdateSet.put(setAttribute.getTableVariable().getAttributeName(), compiledExpression);
            Map<String, ExpressionExecutor> expressionExecutorMap =
                    expressionBuilder.getVariableExpressionExecutorMap();
            parentExecutorMap.putAll(expressionExecutorMap);
        }
        recordTableCompiledUpdateSet.setExpressionExecutorMap(parentExecutorMap);
        if (isCacheEnabled) {
            CompiledUpdateSet cacheCompileUpdateSet =  cachedTable.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
            CompiledUpdateSetWithCache compiledUpdateSetWithCache = new CompiledUpdateSetWithCache(
                    recordTableCompiledUpdateSet, cacheCompileUpdateSet);
            return compiledUpdateSetWithCache;
        }
        return recordTableCompiledUpdateSet;
    }

    /**
     * Class to wrap store compile update set and cache compile update set
     */
    public class CompiledUpdateSetWithCache implements CompiledUpdateSet {
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

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        ExpressionBuilder expressionBuilder = new ExpressionBuilder(condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
        CompiledCondition compileCondition = compileCondition(expressionBuilder);
        Map<String, ExpressionExecutor> expressionExecutorMap = expressionBuilder.getVariableExpressionExecutorMap();

        if (isCacheEnabled) {
            CompiledCondition compiledConditionWithCache = new CompiledConditionWithCache(compileCondition,
                    generateCacheCompileCondition(condition, matchingMetaInfoHolder, siddhiQueryContext,
                            variableExpressionExecutors));
            return new RecordStoreCompiledCondition(expressionExecutorMap, compiledConditionWithCache);
        } else {
            return new RecordStoreCompiledCondition(expressionExecutorMap, compileCondition);
        }
    }

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        if (isCacheEnabled) {

            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionAggregation = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.
                    variableExpressionExecutorMap, compiledConditionAggregation.getStoreCompileCondition());

            StreamEvent cacheResults = findInCache(compiledConditionAggregation.getCacheCompileCondition(),
                    matchingEvent);
            if (cacheResults != null) {
                return cacheResults;
            }
        } else {
            recordStoreCompiledCondition =
                    ((RecordStoreCompiledCondition) compiledCondition);
        }

        Map<String, Object> findConditionParameterMap = new HashMap<>();
        for (Map.Entry<String, ExpressionExecutor> entry : recordStoreCompiledCondition.variableExpressionExecutorMap
                .entrySet()) {
            findConditionParameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }

        Iterator<Object[]> records;
        if (recordTableHandler != null) {
            records = recordTableHandler.find(matchingEvent.getTimestamp(), findConditionParameterMap,
                    recordStoreCompiledCondition.compiledCondition);
        } else {
            records = find(findConditionParameterMap, recordStoreCompiledCondition.compiledCondition);
        }
        ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>(true);
        if (records != null) {
            while (records.hasNext()) {
                Object[] record = records.next();
                StreamEvent streamEvent = storeEventPool.newInstance();
                System.arraycopy(record, 0, streamEvent.getOutputData(), 0, record.length);
                streamEventComplexEventChunk.add(streamEvent);
            }
        }
        return streamEventComplexEventChunk.getFirst();

    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {

        ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>(true);
        RecordStoreCompiledCondition recordStoreCompiledCondition;
        RecordStoreCompiledSelection recordStoreCompiledSelection;

        if (isCacheEnabled) {
            RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
            CompiledConditionWithCache compiledConditionWithCache = (CompiledConditionWithCache)
                    compiledConditionTemp.compiledCondition;
            recordStoreCompiledCondition = new RecordStoreCompiledCondition(
                    compiledConditionTemp.variableExpressionExecutorMap,
                    compiledConditionWithCache.getStoreCompileCondition());

            CompiledSelectionWithCache compiledSelectionWithCache = (CompiledSelectionWithCache) compiledSelection;
            recordStoreCompiledSelection = compiledSelectionWithCache.recordStoreCompiledSelection;
            StreamEvent cacheResults = findInCache(compiledConditionWithCache.getCacheCompileCondition(),
                    matchingEvent);
            if (cacheResults != null) {
                StateEventFactory stateEventFactory = new StateEventFactory(compiledSelectionWithCache.
                        metaStreamInfoHolder.getMetaStateEvent());
                Event[] cacheResultsAfterSelection = executeSelector(cacheResults,
                        compiledSelectionWithCache.querySelector,
                        stateEventFactory, MetaStreamEvent.EventType.TABLE);
                assert cacheResultsAfterSelection != null;
                for (Event event : cacheResultsAfterSelection) {

                    Object[] record = event.getData();
                    StreamEvent streamEvent = storeEventPool.newInstance();
                    streamEvent.setOutputData(new Object[outputAttributes.length]);
                    System.arraycopy(record, 0, streamEvent.getOutputData(), 0, record.length);
                    streamEventComplexEventChunk.add(streamEvent);
                }
                return streamEventComplexEventChunk.getFirst();
            }
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
        if (recordTableHandler != null) {
            records = recordTableHandler.query(matchingEvent.getTimestamp(), parameterMap,
                    recordStoreCompiledCondition.compiledCondition,
                    recordStoreCompiledSelection.compiledSelection);
        } else {
            records = query(parameterMap, recordStoreCompiledCondition.compiledCondition,
                    recordStoreCompiledSelection.compiledSelection, outputAttributes);
        }
        if (records != null) {
            while (records.hasNext()) {
                Object[] record = records.next();
                StreamEvent streamEvent = storeEventPool.newInstance();
                streamEvent.setOutputData(new Object[outputAttributes.length]);
                System.arraycopy(record, 0, streamEvent.getOutputData(), 0, record.length);
                streamEventComplexEventChunk.add(streamEvent);
            }
        }
        return streamEventComplexEventChunk.getFirst();
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
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
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
                        variableExpressionExecutors, tableMap, siddhiQueryContext));
            }
        }

        ExpressionBuilder havingExpressionBuilder = null;
        if (selector.getHavingExpression() != null) {
            havingExpressionBuilder = new ExpressionBuilder(selector.getHavingExpression(), metaInfoHolderAfterSelect,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
        }

        List<OrderByAttributeBuilder> orderByAttributeBuilders = null;
        if (selector.getOrderByList().size() != 0) {
            orderByAttributeBuilders = new ArrayList<>(selector.getOrderByList().size());
            for (OrderByAttribute orderByAttribute : selector.getOrderByList()) {
                ExpressionBuilder expressionBuilder = new ExpressionBuilder(orderByAttribute.getVariable(),
                        metaInfoHolderAfterSelect, variableExpressionExecutors,
                        tableMap, siddhiQueryContext);
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

        if (isCacheEnabled) {
            CompiledSelectionWithCache compiledSelectionWithCache;

            ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
            int metaPosition = SiddhiConstants.UNKNOWN_STATE;
            List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector = new ArrayList<>();
            QuerySelector querySelector = SelectorParser.parse(selector,
                    returnStream,
                    matchingMetaInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutorsForQuerySelector,
                    metaPosition, ProcessingMode.BATCH, false, siddhiQueryContext);
            QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                    variableExpressionExecutorsForQuerySelector);
            querySelector.setEventPopulator(
                    StateEventPopulatorFactory.constructEventPopulator(matchingMetaInfoHolder.getMetaStateEvent()));

            RecordStoreCompiledSelection recordStoreCompiledSelection =
                    new RecordStoreCompiledSelection(expressionExecutorMap, compiledSelection);

            compiledSelectionWithCache = new CompiledSelectionWithCache(recordStoreCompiledSelection, querySelector,
                    matchingMetaInfoHolder);
            return compiledSelectionWithCache;
        } else {
            return  new RecordStoreCompiledSelection(expressionExecutorMap, compiledSelection);
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

    /**
     * Class to hold store compile condition and cache compile condition wrapped
     */
    private class CompiledConditionWithCache implements CompiledCondition {
        CompiledCondition cacheCompileCondition;
        CompiledCondition storeCompileCondition;

        public CompiledConditionWithCache(CompiledCondition storeCompileCondition,
                                          CompiledCondition cacheCompileCondition) {
            this.storeCompileCondition = storeCompileCondition;
            this.cacheCompileCondition = cacheCompileCondition;
        }

        public CompiledCondition getStoreCompileCondition() {
            return storeCompileCondition;
        }

        public CompiledCondition getCacheCompileCondition() {
            return cacheCompileCondition;
        }
    }

    /**
     * class to hold both store compile selection and cache compile selection wrapped
     */
    public class CompiledSelectionWithCache implements CompiledSelection {
        QuerySelector querySelector;
        MatchingMetaInfoHolder metaStreamInfoHolder;
        RecordStoreCompiledSelection recordStoreCompiledSelection;

        public QuerySelector getQuerySelector() {
            return querySelector;
        }

        CompiledSelectionWithCache(RecordStoreCompiledSelection recordStoreCompiledSelection,
                                   QuerySelector querySelector,
                                   MatchingMetaInfoHolder metaStreamInfoHolder) {
            this.recordStoreCompiledSelection = recordStoreCompiledSelection;
            this.querySelector = querySelector;
            this.metaStreamInfoHolder = metaStreamInfoHolder;
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

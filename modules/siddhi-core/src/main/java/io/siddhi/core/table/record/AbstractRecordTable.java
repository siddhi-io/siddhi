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

package io.siddhi.core.table.record;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.DatabaseRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.expression.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An abstract implementation of table. Abstract implementation will handle {@link ComplexEventChunk} so that
 * developer can directly work with event data.
 */
public abstract class AbstractRecordTable extends Table {
    private static final Logger log = LogManager.getLogger(AbstractRecordTable.class);
    protected StreamEventFactory storeEventPool;
    protected RecordTableHandler recordTableHandler;
    private ThreadLocal<DynamicOptions> trpDynamicOptions;

    @Override
    public void init(TableDefinition tableDefinition, StreamEventFactory storeEventPool,
                     StreamEventCloner storeEventCloner, ConfigReader configReader, SiddhiAppContext
                             siddhiAppContext, RecordTableHandler recordTableHandler) {
        if (recordTableHandler != null) {
            recordTableHandler.init(tableDefinition, new RecordTableHandlerCallback(this),
                    siddhiAppContext);
        }
        this.recordTableHandler = recordTableHandler;
        this.storeEventPool = storeEventPool;
        init(tableDefinition, configReader);
        initCache(tableDefinition, siddhiAppContext, storeEventCloner, configReader);
    }

    protected void initCache(TableDefinition tableDefinition, SiddhiAppContext siddhiAppContext,
                             StreamEventCloner storeEventCloner, ConfigReader configReader) {

    }

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definition of the table with annotations if any
     * @param configReader    this hold the {@link AbstractRecordTable} configuration reader.
     */
    protected abstract void init(TableDefinition tableDefinition, ConfigReader configReader);

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        List<Object[]> records = new ArrayList<>();
        addingEventChunk.reset();
        long timestamp = 0L;
        while (addingEventChunk.hasNext()) {
            StreamEvent event = addingEventChunk.next();
            records.add(event.getOutputData());
            timestamp = event.getTimestamp();
        }
        try {
            if (recordTableHandler != null) {
                recordTableHandler.add(timestamp, records);
            } else {
                add(records);
            }
        } catch (ConnectionUnavailableException | DatabaseRuntimeException e) {
            onAddError(addingEventChunk, e);
        }
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     * @throws ConnectionUnavailableException
     */
    protected abstract void add(List<Object[]> records) throws ConnectionUnavailableException;

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition =
                ((RecordStoreCompiledCondition) compiledCondition);

        Map<String, Object> findConditionParameterMap = new HashMap<>();
        for (Map.Entry<String, ExpressionExecutor> entry : recordStoreCompiledCondition.variableExpressionExecutorMap
                .entrySet()) {
            findConditionParameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }

        Iterator<Object[]> records;
        if (recordTableHandler != null) {
            records = recordTableHandler.find(matchingEvent.getTimestamp(), findConditionParameterMap,
                    recordStoreCompiledCondition.getCompiledCondition());
        } else {
            records = find(findConditionParameterMap, recordStoreCompiledCondition.getCompiledCondition());
        }
        ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>();
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

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values
     *                                  corresponding to the compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     * @throws ConnectionUnavailableException
     */
    protected abstract RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                                     CompiledCondition compiledCondition)
            throws ConnectionUnavailableException;

    @Override
    public boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        RecordStoreCompiledCondition recordStoreCompiledCondition =
                ((RecordStoreCompiledCondition) compiledCondition);
        Map<String, Object> containsConditionParameterMap = new HashMap<>();
        for (Map.Entry<String, ExpressionExecutor> entry :
                recordStoreCompiledCondition.variableExpressionExecutorMap.entrySet()) {
            containsConditionParameterMap.put(entry.getKey(), entry.getValue().execute(matchingEvent));
        }
        if (recordTableHandler != null) {
            return recordTableHandler.contains(matchingEvent.getTimestamp(), containsConditionParameterMap,
                    recordStoreCompiledCondition.getCompiledCondition());
        } else {
            return contains(containsConditionParameterMap, recordStoreCompiledCondition.getCompiledCondition());
        }
    }

    /**
     * Check if matching record exist
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     * @throws ConnectionUnavailableException
     */
    protected abstract boolean contains(Map<String, Object> containsConditionParameterMap,
                                        CompiledCondition compiledCondition)
            throws ConnectionUnavailableException;

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition) {
        RecordStoreCompiledCondition recordStoreCompiledCondition =
                ((RecordStoreCompiledCondition) compiledCondition);
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
        try {
            if (recordTableHandler != null) {
                recordTableHandler.delete(timestamp, deleteConditionParameterMaps, recordStoreCompiledCondition.
                        getCompiledCondition());
            } else {
                delete(deleteConditionParameterMaps, recordStoreCompiledCondition.getCompiledCondition());
            }
        } catch (ConnectionUnavailableException | DatabaseRuntimeException e) {
            onDeleteError(deletingEventChunk, compiledCondition, e);
        }

    }

    protected void connectAndLoadCache() throws ConnectionUnavailableException {
        connect();
    }

    protected abstract void connect() throws ConnectionUnavailableException;


    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @throws ConnectionUnavailableException
     */
    protected abstract void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                                   CompiledCondition compiledCondition)
            throws ConnectionUnavailableException;

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) {
        RecordStoreCompiledCondition recordStoreCompiledCondition =
                ((RecordStoreCompiledCondition) compiledCondition);
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet) compiledUpdateSet;
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
        try {
            if (recordTableHandler != null) {
                recordTableHandler.update(timestamp, recordStoreCompiledCondition.getCompiledCondition(),
                        updateConditionParameterMaps, recordTableCompiledUpdateSet.getUpdateSetMap(),
                        updateSetParameterMaps);
            } else {
                update(recordStoreCompiledCondition.getCompiledCondition(), updateConditionParameterMaps,
                        recordTableCompiledUpdateSet.getUpdateSetMap(), updateSetParameterMaps);
            }
        } catch (ConnectionUnavailableException | DatabaseRuntimeException e) {
            onUpdateError(updatingEventChunk, compiledCondition, compiledUpdateSet, e
            );
        }
    }


    /**
     * Update all matching records
     *
     * @param updateCondition              the compiledCondition against which records should be matched for update
     * @param updateConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition based on which the records will be updated
     * @param updateSetExpressions         the set of updates mappings and related complied expressions
     * @param updateSetParameterMaps       map of matching StreamVariable Ids and their values corresponding to the
     * @throws ConnectionUnavailableException
     */
    protected abstract void update(CompiledCondition updateCondition,
                                   List<Map<String, Object>> updateConditionParameterMaps,
                                   Map<String, CompiledExpression> updateSetExpressions,
                                   List<Map<String, Object>> updateSetParameterMaps)
            throws ConnectionUnavailableException;

    @Override
    public void updateOrAdd(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                            CompiledCondition compiledCondition, CompiledUpdateSet compiledUpdateSet,
                            AddingStreamEventExtractor addingStreamEventExtractor) {
        RecordStoreCompiledCondition recordStoreCompiledCondition =
                ((RecordStoreCompiledCondition) compiledCondition);
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet = (RecordTableCompiledUpdateSet) compiledUpdateSet;
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
        try {
            if (recordTableHandler != null) {
                recordTableHandler.updateOrAdd(timestamp, recordStoreCompiledCondition.getCompiledCondition(),
                        updateConditionParameterMaps, recordTableCompiledUpdateSet.getUpdateSetMap(),
                        updateSetParameterMaps, addingRecords);
            } else {
                updateOrAdd(recordStoreCompiledCondition.getCompiledCondition(), updateConditionParameterMaps,
                        recordTableCompiledUpdateSet.getUpdateSetMap(), updateSetParameterMaps, addingRecords);
            }
        } catch (ConnectionUnavailableException | DatabaseRuntimeException e) {
            onUpdateOrAddError(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                    addingStreamEventExtractor, e);
        }
    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param updateCondition              the compiledCondition against which records should be matched for update
     * @param updateConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition based on which the records will be updated
     * @param updateSetExpressions         the set of updates mappings and related complied expressions
     * @param updateSetParameterMaps       map of matching StreamVariable Ids and their values corresponding to the
     *                                     update set
     * @param addingRecords                the values for adding new records if the update condition did not match
     * @throws ConnectionUnavailableException
     */
    protected abstract void updateOrAdd(CompiledCondition updateCondition,
                                        List<Map<String, Object>> updateConditionParameterMaps,
                                        Map<String, CompiledExpression> updateSetExpressions,
                                        List<Map<String, Object>> updateSetParameterMaps,
                                        List<Object[]> addingRecords)
            throws ConnectionUnavailableException;

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        ExpressionExecutor inMemoryCompiledCondition = ExpressionParser.parseExpression(condition,
                matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                variableExpressionExecutors, false, 0,
                ProcessingMode.BATCH, false, siddhiQueryContext);
        ExpressionBuilder expressionBuilder = new ExpressionBuilder(
                condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap,
                new UpdateOrInsertReducer(inMemoryCompiledCondition, matchingMetaInfoHolder), null,
                siddhiQueryContext);
        CompiledCondition compileCondition = compileCondition(expressionBuilder);
        Map<String, ExpressionExecutor> expressionExecutorMap = expressionBuilder.getVariableExpressionExecutorMap();
        return new RecordStoreCompiledCondition(expressionExecutorMap, compileCondition, siddhiQueryContext);
    }

    public CompiledUpdateSet compileUpdateSet(UpdateSet updateSet,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        RecordTableCompiledUpdateSet recordTableCompiledUpdateSet = new RecordTableCompiledUpdateSet();
        Map<String, ExpressionExecutor> parentExecutorMap = new HashMap<>();
        for (UpdateSet.SetAttribute setAttribute : updateSet.getSetAttributeList()) {
            ExpressionExecutor inMemoryAssignmentExecutor = ExpressionParser.parseExpression(
                    setAttribute.getAssignmentExpression(), matchingMetaInfoHolder.getMetaStateEvent(),
                    matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH, false,
                    siddhiQueryContext);
            ExpressionBuilder expressionBuilder = new ExpressionBuilder(setAttribute.getAssignmentExpression(),
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, null,
                    inMemoryAssignmentExecutor, siddhiQueryContext);
            CompiledExpression compiledExpression = compileSetAttribute(expressionBuilder);
            recordTableCompiledUpdateSet.put(setAttribute.getTableVariable().getAttributeName(), compiledExpression);
            Map<String, ExpressionExecutor> expressionExecutorMap =
                    expressionBuilder.getVariableExpressionExecutorMap();
            parentExecutorMap.putAll(expressionExecutorMap);
        }
        recordTableCompiledUpdateSet.setExpressionExecutorMap(parentExecutorMap);
        return recordTableCompiledUpdateSet;
    }

    /**
     * Compile the matching expression
     *
     * @param expressionBuilder helps visiting the conditions in order to compile the condition
     * @return compiled expression that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    protected abstract CompiledCondition compileCondition(ExpressionBuilder expressionBuilder);

    /**
     * Compiles the expression in a set clause
     *
     * @param expressionBuilder helps visiting the conditions in order to compile the condition
     * @return compiled expression that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    protected abstract CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder);

    @Override
    public boolean isStateful() {
        return false;
    }
}

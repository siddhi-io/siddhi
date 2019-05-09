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
package io.siddhi.core.query.table.util;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.InMemoryCompiledUpdateSet;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.holder.EventHolder;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.EventHolderPasser;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Custom store for testing of in memory cache for store tables.
 */
@Extension(
        name = "testWithCache",
        namespace = "store",
        description = "Using this implementation a testing for store extension can be done.",
        examples = {
                @Example(
                        syntax = "@store(type='test')" +
                                "define table testTable (symbol string, price int, volume float); ",
                        description = "The above syntax initializes a test type store."
                )
        }
)
public class TestStoreWithCache extends AbstractQueryableRecordTable {
    private InMemoryTable inMemoryTable;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        inMemoryTable = new InMemoryTable();

        MetaStreamEvent cacheTableMetaStreamEvent = new MetaStreamEvent();
        cacheTableMetaStreamEvent.addInputDefinition(tableDefinition);
        for (Attribute attribute : tableDefinition.getAttributeList()) {
            cacheTableMetaStreamEvent.addOutputData(attribute);
        }

//        StreamEventFactory testTableStreamEventFactory = new StreamEventFactory(cacheTableMetaStreamEvent);
        StreamEventCloner testTableStreamEventCloner = new StreamEventCloner(cacheTableMetaStreamEvent,
                storeEventPool);

        inMemoryTable.init(generateCacheTableDefinition(tableDefinition), storeEventPool, testTableStreamEventCloner, configReader,
                super.getSiddhiAppContext(), recordTableHandler);
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {

    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        return this.find(parameterMap, compiledCondition);
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {
        return null;
    }

    private ComplexEventChunk<StreamEvent> objectListToComplexEventChunk(List<Object[]> records) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
        for (Object[] record: records) {
//            StreamEvent event = new StreamEvent(0, 0, record.length);
            StreamEvent event = storeEventPool.newInstance();
            event.setOutputData(record);
            complexEventChunk.add(event);
        }
        return complexEventChunk;
    }

    private ComplexEventChunk<StateEvent> objectListToComplexEventChunkStateEvent(List<Object[]> records) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>(false);
        for (Object[] record: records) {
            StreamEvent event = new StreamEvent(0, 0, record.length);
            StateEvent stateEvent = new StateEvent(2, record.length);
            event.setOutputData(record);
            stateEvent.addEvent(0, event);
            complexEventChunk.add(stateEvent);
        }
        return complexEventChunk;
    }

    private ComplexEventChunk<StreamEvent> parameterMapToComplexEventChunk(Map<String, Object> parameterMap) {
        List<Object[]> objectList = new LinkedList<>();
        objectList.add(parameterMap.values().toArray());
        return objectListToComplexEventChunk(objectList);
    }

    private ComplexEventChunk<StateEvent> parameterMapListToComplexEventChunk(
            List<Map<String, Object>> parameterMapList) {
        List<Object[]> objectList = new LinkedList<>();
        for (Map<String, Object> parameterMap: parameterMapList) {
            objectList.add(parameterMap.values().toArray());
        }
        return objectListToComplexEventChunkStateEvent(objectList);
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        inMemoryTable.add(objectListToComplexEventChunk(records));
    }

    private StateEvent parameterMapToStateEvent(Map<String, Object> parameterMap) {
        StateEvent stateEvent = new StateEvent(2, parameterMap.size());
        Object[] outputData = parameterMap.values().toArray();
        StreamEvent event = storeEventPool.newInstance();
        event.setOutputData(outputData);
        stateEvent.addEvent(0, event);
        return stateEvent;
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        StreamEvent outEvent = inMemoryTable.find(compiledCondition,
                parameterMapToStateEvent(findConditionParameterMap));

        List<Object[]> objects = new LinkedList<>();

        if (outEvent != null) {
            while (outEvent.hasNext()) {
                objects.add(outEvent.getOutputData());
                outEvent = outEvent.getNext();
            }
            objects.add(outEvent.getOutputData());
        }

        return new TestStoreWithCacheIterator(objects.iterator());
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        return inMemoryTable.contains(parameterMapToStateEvent(containsConditionParameterMap), compiledCondition);
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        ComplexEventChunk<StateEvent> deletingChunk = parameterMapListToComplexEventChunk(deleteConditionParameterMaps);
        inMemoryTable.delete(deletingChunk, compiledCondition);
    }

    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions,
                          List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {

    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingRecords) throws ConnectionUnavailableException {

    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        TestStoreConditionVisitor visitor = new TestStoreConditionVisitor(inMemoryTable.getTableDefinition().getId());
        expressionBuilder.build(visitor);
        return inMemoryTable.compileCondition(expressionBuilder.getExpression(),
                expressionBuilder.getMatchingMetaInfoHolder(), expressionBuilder.getVariableExpressionExecutors(),
                expressionBuilder.getTableMap(), expressionBuilder.getSiddhiQueryContext());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
//        Map<Integer, ExpressionExecutor> expressionExecutorMap = new HashMap<>();
//        for (UpdateSet.SetAttribute setAttribute : updateSet.getSetAttributeList()) {
//            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(
//                    setAttribute.getAssignmentExpression(), matchingMetaInfoHolder.getMetaStateEvent(),
//                    matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
//                    false, 0, ProcessingMode.BATCH, false,
//                    siddhiQueryContext);
//            int attributePosition = tableDefinition.
//                    getAttributePosition(setAttribute.getTableVariable().getAttributeName());
//            expressionExecutorMap.put(attributePosition, expressionExecutor);
//        }
//        return new InMemoryCompiledUpdateSet(expressionExecutorMap);
        return null;
    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }
}

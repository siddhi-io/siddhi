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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.MatcherParser;
import io.siddhi.core.util.parser.SelectorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.expression.Variable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.siddhi.core.util.StoreQueryRuntimeUtil.executeSelector;

/**
 * Custom store for testing of in memory cache for store tables.
 */
@Extension(
        name = "testStoreContainingInMemoryTable",
        namespace = "store",
        description = "Using this implementation a testing for store extension can be done.",
        examples = {
                @Example(
                        syntax = "@store(type='testStoreContainingInMemoryTable')" +
                                "define table testTable (symbol string, price int, volume float); ",
                        description = "The above syntax initializes a test type store."
                )
        }
)
public class TestStoreContainingInMemoryTable extends AbstractQueryableRecordTable {
    private InMemoryTable inMemoryTable;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        inMemoryTable = new InMemoryTable();

        MetaStreamEvent cacheTableMetaStreamEvent = new MetaStreamEvent();
        cacheTableMetaStreamEvent.addInputDefinition(tableDefinition);
        for (Attribute attribute : tableDefinition.getAttributeList()) {
            cacheTableMetaStreamEvent.addOutputData(attribute);
        }
        StreamEventCloner testTableStreamEventCloner = new StreamEventCloner(cacheTableMetaStreamEvent,
                storeEventPool);

        inMemoryTable.init(generateCacheTableDefinition(tableDefinition), storeEventPool, testTableStreamEventCloner,
                configReader, super.getSiddhiAppContext(), recordTableHandler);
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {

    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes) {
        StreamEvent outEvent = inMemoryTable.find(compiledCondition, findMatchingEvent);
        List<Object[]> objects = new LinkedList<>();
        CompiledSelectionWithCache compiledSelectionWithCache = null;

        if (outEvent != null) {
            compiledSelectionWithCache = (CompiledSelectionWithCache) compiledSelection;
            StateEventFactory stateEventFactory = new StateEventFactory(compiledSelectionWithCache.
                    getMetaStreamInfoHolder().getMetaStateEvent());
            Event[] cacheResultsAfterSelection = executeSelector(outEvent,
                    compiledSelectionWithCache.getQuerySelector(), stateEventFactory, MetaStreamEvent.EventType.TABLE);

            if (compiledSelectionWithCache.getQuerySelector() !=  null &
                    compiledSelectionWithCache.getQuerySelector().getAttributeProcessorList().size() !=  0) {
                compiledSelectionWithCache.getQuerySelector().
                        process(generateResetComplexEventChunk(outEvent.getOutputData().length, stateEventFactory));
            }

            if (cacheResultsAfterSelection != null) {
                for (Event event: cacheResultsAfterSelection) {
                    objects.add(event.getData());
                }
            }
        }
        return new TestStoreWithCacheIterator(objects.iterator());
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {
        CompiledSelectionWithCache compiledSelectionWithCache;

        ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
        int metaPosition = SiddhiConstants.UNKNOWN_STATE;
        List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector = new ArrayList<>();
//        Selector selector = new Selector();

//        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
//        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
//        TableDefinition matchingTableDefinition = TableDefinition.id("");
//        for (Attribute attribute : inMemoryTable.getTableDefinition().getAttributeList()) {
//            tableMetaStreamEvent.addOutputData(attribute);
//            matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
//        }
//        tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);
//        MatchingMetaInfoHolder matchingMetaInfoHolder =
//                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
//                        inMemoryTable.getTableDefinition(), 0);

//        SiddhiQueryContext siddhiQueryContext = siddhiAppContext.

        QuerySelector querySelector = SelectorParser.parse(selectorForTestStoreQuery,
                returnStream,
                matchingMetaInfoHolderForTestStoreQuery.getMetaStateEvent(), tableMap,
                variableExpressionExecutorsForQuerySelector, metaPosition, ProcessingMode.BATCH,
                false, siddhiQueryContextForTestStoreQuery);
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolderForTestStoreQuery.
                        getMetaStateEvent(),
                variableExpressionExecutorsForQuerySelector);
        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(matchingMetaInfoHolderForTestStoreQuery.
                        getMetaStateEvent()));

        compiledSelectionWithCache = new CompiledSelectionWithCache(null, querySelector,
                matchingMetaInfoHolderForTestStoreQuery);
        return compiledSelectionWithCache;
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        inMemoryTable.add(objectListToComplexEventChunk(records));
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        if (findMatchingEvent == null) {
            findMatchingEvent = new StateEvent(1, 0);
        }
        try {
            StreamEvent outEvent = inMemoryTable.find(compiledCondition, findMatchingEvent);
            List<Object[]> objects = new LinkedList<>();
            if (outEvent != null) {
                while (outEvent.hasNext()) {
                    objects.add(outEvent.getOutputData());
                    outEvent = outEvent.getNext();
                }
                objects.add(outEvent.getOutputData());
            }
            return new TestStoreWithCacheIterator(objects.iterator());
        } finally {
            findMatchingEvent = null;
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        return inMemoryTable.contains(containsMatchingEvent, compiledCondition);
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        inMemoryTable.delete(convForDelete(deleteConditionParameterMaps), compiledCondition);
    }

    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions,
                          List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        TableDefinition matchingTableDefinition = TableDefinition.id("");
        for (Attribute attribute : inMemoryTable.getTableDefinition().getAttributeList()) {
            tableMetaStreamEvent.addOutputData(attribute);
            matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);
        MatchingMetaInfoHolder matchingMetaInfoHolder =
                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                        inMemoryTable.getTableDefinition(), 0);

        UpdateSet updateSet = new UpdateSet();
        for (Attribute attribute : matchingMetaInfoHolder.getMatchingStreamDefinition().
                getAttributeList()) {
            updateSet.set(new Variable(attribute.getName()), new Variable(attribute.getName()));
        }

        CompiledUpdateSet compiledUpdateSet = inMemoryTable.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                null, tableMap, null);

        inMemoryTable.update(convForUpdate(updateSetParameterMaps), updateCondition,
                compiledUpdateSet);
    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingRecords) throws ConnectionUnavailableException {

        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        TableDefinition matchingTableDefinition = TableDefinition.id("");
        for (Attribute attribute : inMemoryTable.getTableDefinition().getAttributeList()) {
            tableMetaStreamEvent.addOutputData(attribute);
            matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);
        MatchingMetaInfoHolder matchingMetaInfoHolder =
                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                        inMemoryTable.getTableDefinition(), 0);

        UpdateSet updateSet = new UpdateSet();
        for (Attribute attribute : matchingMetaInfoHolder.getMatchingStreamDefinition().
                getAttributeList()) {
            updateSet.set(new Variable(attribute.getName()), new Variable(attribute.getName()));
        }

        CompiledUpdateSet compiledUpdateSet = inMemoryTable.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                null, tableMap, null);
        inMemoryTable.updateOrAdd(convForUpdate(updateSetParameterMaps), updateCondition, compiledUpdateSet,
                new AddingStreamEventExtractor(matchingMetaInfoHolder.getMatchingStreamEventIndex()));
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        TestStoreConditionVisitor visitor = new TestStoreConditionVisitor(inMemoryTable.getTableDefinition().getId());
        expressionBuilder.build(visitor);
        if (expressionBuilder.getSiddhiQueryContext().getName().startsWith("store_select_query_")) {
            expressionBuilder.getMatchingMetaInfoHolder().getMetaStateEvent().getMetaStreamEvent(0).
                    setEventType(MetaStreamEvent.EventType.TABLE);
        }
        return inMemoryTable.compileCondition(expressionBuilder.getExpression(),
                expressionBuilder.getMatchingMetaInfoHolder(), expressionBuilder.getVariableExpressionExecutors(),
                expressionBuilder.getTableMap(), expressionBuilder.getSiddhiQueryContext());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    private ComplexEventChunk<ComplexEvent> generateResetComplexEventChunk(int outputDataSize,
                                                                           StateEventFactory stateEventFactory) {
        StreamEvent streamEvent = new StreamEvent(outputDataSize, 0, outputDataSize);
        streamEvent.setType(ComplexEvent.Type.RESET);

        StateEvent stateEvent = stateEventFactory.newInstance();
        stateEvent.addEvent(0, streamEvent);
        stateEvent.setType(ComplexEvent.Type.RESET);

        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>(true);
        complexEventChunk.add(stateEvent);
        return complexEventChunk;
    }

    private ComplexEventChunk<StateEvent> convForUpdate(List<Map<String, Object>> parameterMapList) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>(false);
        for (Map<String, Object> parameterMap: parameterMapList) {
            complexEventChunk.add(parameterMapToStateEvent(parameterMap));
        }
        return complexEventChunk;
    }

    private ComplexEventChunk<StreamEvent> objectListToComplexEventChunk(List<Object[]> records) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
        for (Object[] record: records) {
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

//    private ComplexEventChunk<StreamEvent> parameterMapToComplexEventChunk(Map<String, Object> parameterMap) {
//        List<Object[]> objectList = new LinkedList<>();
//        objectList.add(parameterMap.values().toArray());
//        return objectListToComplexEventChunk(objectList);
//    }
//
//    private ComplexEventChunk<StateEvent> parameterMapListToComplexEventChunk(
//            List<Map<String, Object>> parameterMapList) {
//        List<Object[]> objectList = new LinkedList<>();
//        for (Map<String, Object> parameterMap: parameterMapList) {
//            objectList.add(parameterMap.values().toArray());
//        }
//        return objectListToComplexEventChunkStateEvent(objectList);
//    }

    private StateEvent convForContains(Map<String, Object> parameterMap) {
        StateEvent stateEvent = new StateEvent(2, parameterMap.size());
        List<Object> outputData = new ArrayList<>();
        List<Attribute> attributeList = inMemoryTable.getTableDefinition().getAttributeList();

        for (int i = 0; i < attributeList.size(); i++) {
            if (parameterMap.get(attributeList.get(i).getName()) != null) {
                outputData.add(parameterMap.get(attributeList.get(i).getName()));
//            } else {
//                outputData.add(null);
            }
        }

        StreamEvent event = storeEventPool.newInstance();
        event.setOutputData(outputData.toArray());
        stateEvent.addEvent(0, event);
        return stateEvent;
    }

//    private StateEvent convForContains2(Map<String, Object> parameterMap, CompiledCondition compiledCondition) {
//        StateEvent stateEvent = new StateEvent(2, parameterMap.size());
//        List<Object> outputData = new ArrayList<>();
//        List<Attribute> attributeList = inMemoryTable.getTableDefinition().getAttributeList();
//
//        for (int i = 0; i < attributeList.size(); i++) {
//            if (parameterMap.get(attributeList.get(i).getName()) != null) {
//                outputData.add(parameterMap.get(attributeList.get(i).getName()));
////            } else {
////                outputData.add(null);
//            }
//        }
//
//        StreamEvent event = storeEventPool.newInstance();
//        event.setOutputData(outputData.toArray());
//        stateEvent.addEvent(0, event);
//        return stateEvent;
//    }

//    private String[] getParameterOrderFromCompileCondition(CompiledCondition compiledCondition) {
//        SnapshotableEventQueueOperator operator = (SnapshotableEventQueueOperator) compiledCondition;
//        int streamEventPosition = 0;
//        if (operator.)
//    }

    private ComplexEventChunk<StateEvent> convForDelete (List<Map<String, Object>> deleteConditionParameterMaps) {
        List<Object[]> objectList = new LinkedList<>();
        for (Map<String, Object> parameterMap: deleteConditionParameterMaps) {
            List<Object> outputData = new ArrayList<>();
            List<Attribute> attributeList = inMemoryTable.getTableDefinition().getAttributeList();

            for (int i = 0; i < attributeList.size(); i++) {
                if (parameterMap.get(attributeList.get(i).getName()) != null) {
                    outputData.add(parameterMap.get(attributeList.get(i).getName()));
                } else {
                    outputData.add(null);
                }
            }
            objectList.add(outputData.toArray());
        }

        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>(false);
        for (Object[] record: objectList) {
            StreamEvent event = new StreamEvent(0, 0, record.length);
            StateEvent stateEvent = new StateEvent(2, record.length);
            event.setOutputData(record);
            stateEvent.addEvent(0, event);
            complexEventChunk.add(stateEvent);
        }
        return complexEventChunk;
    }

    private StateEvent parameterMapToStateEvent(Map<String, Object> parameterMap) {
        StateEvent stateEvent = new StateEvent(2, parameterMap.size());
        List<Object> outputData = new ArrayList<>();
        List<Attribute> attributeList = inMemoryTable.getTableDefinition().getAttributeList();

        for (int i = 0; i < attributeList.size(); i++) {
            if (parameterMap.get(attributeList.get(i).getName()) != null) {
                outputData.add(parameterMap.get(attributeList.get(i).getName()));
            } else {
                outputData.add(null);
            }
        }

        StreamEvent event = storeEventPool.newInstance();
        event.setOutputData(outputData.toArray());
        stateEvent.addEvent(0, event);
        return stateEvent;
    }

//    private StateEvent convForFind(Map<String, Object> parameterMap) {
//        StateEvent stateEvent = new StateEvent(2, parameterMap.size());
//        List<Object> outputData = new ArrayList<>();
//        List<Attribute> attributeList = inMemoryTable.getTableDefinition().getAttributeList();
//
//        Set<String> keys = parameterMap.keySet();
//
////        for (String key: keys) {
////            String[] parts = key.split("\\.");
////            int index = -1;
////            for (int i = 0; i < attributeList.size(); i++) {
////                if (attributeList.get(i).getName().equalsIgnoreCase(parts[parts.length - 1])) {
////                    index = i;
////                }
////            }
////            if (index != -1) {
////                outputData.add(index, parameterMap.get(parts[parts.length - 1]));
////            } else {
////                outputData.add(null);
////            }
////        }
//
//        for (int i = 0; i < attributeList.size(); i++) {
//            if (getValueFromParameterMap(attributeList.get(i).getName(), parameterMap) != null) {
//                outputData.add(getValueFromParameterMap(attributeList.get(i).getName(), parameterMap));
//            } else {
//                outputData.add(null);
//            }
//        }
//
//        StreamEvent event = storeEventPool.newInstance();
//        event.setOutputData(outputData.toArray());
//        stateEvent.addEvent(0, event);
//        return stateEvent;
//    }

//    protected Object getValueFromParameterMap(String subKey, Map<String, Object> parameterMap) {
//        Set<String> keys = parameterMap.keySet();
//        for (String fullKey: keys) {
//            String[] parts = fullKey.split("\\.");
//            if (subKey.equalsIgnoreCase(parts[parts.length - 1])) {
//                return parameterMap.get(fullKey);
//            }
//        }
//        return null;
//    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }
}

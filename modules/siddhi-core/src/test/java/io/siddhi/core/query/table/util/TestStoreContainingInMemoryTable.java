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
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
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
import io.siddhi.query.api.annotation.Annotation;
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

import static io.siddhi.core.util.OnDemandQueryRuntimeUtil.executeSelector;

/**
 * Custom store for testing of in memory cache for store tables.
 */
@Extension(
        name = "testStoreContainingInMemoryTable",
        deprecated = true,
        deprecationNotice = "store:testStoreContainingInMemoryTable is being deprecated for testing purposes.",
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

        TableDefinition testStoreContainingIMTableDefinition = TableDefinition.id(tableDefinition.getId());
        for (Attribute attribute : tableDefinition.getAttributeList()) {
            testStoreContainingIMTableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        for (Annotation annotation : tableDefinition.getAnnotations()) {
            if (!annotation.getName().equalsIgnoreCase("Store")) {
                testStoreContainingIMTableDefinition.annotation(annotation);
            }
        }

        inMemoryTable.init(testStoreContainingIMTableDefinition, storeEventPool, testTableStreamEventCloner,
                configReader, siddhiAppContext, recordTableHandler);
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
            StateEventFactory stateEventFactory = new StateEventFactory(compiledSelectionWithCache.getMetaStateEvent());
            Event[] cacheResultsAfterSelection = executeSelector(stateEventFactory, null, outEvent,
                    compiledSelectionWithCache.getStoreEventIndex(), compiledSelectionWithCache.getQuerySelector());

            if (compiledSelectionWithCache.getQuerySelector() != null &
                    compiledSelectionWithCache.getQuerySelector().getAttributeProcessorList().size() != 0) {
                compiledSelectionWithCache.getQuerySelector().
                        process(generateResetComplexEventChunk(outEvent.getOutputData().length, stateEventFactory));
            }
            if (cacheResultsAfterSelection != null) {
                for (Event event : cacheResultsAfterSelection) {
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

        selectAttributeBuilders.forEach((selectAttributeBuilder -> {
            TestStoreConditionVisitor testStoreConditionVisitor = new TestStoreConditionVisitor("");
            selectAttributeBuilder.getExpressionBuilder().build(testStoreConditionVisitor);
            if (testStoreConditionVisitor.getStreamVarCount() > 0) {
                throw new SiddhiAppCreationException("testStoreContainingInMemoryTable does not support " +
                        "lookup with stream variables");
            }
        }));

        CompiledSelectionWithCache compiledSelectionWithCache;
        MetaStateEvent metaStateEvent = matchingMetaInfoHolderForTestOnDemandQuery.getMetaStateEvent().clone();

        ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
        int metaPosition = SiddhiConstants.UNKNOWN_STATE;
        List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector = new ArrayList<>();

        if (metaStateEvent.getOutputDataAttributes().size() == 0) {
//            MetaStateEvent metaStateEventWithOutputData = new MetaStateEvent(metaStateEvent.getStreamEventCount());
            for (Attribute outputAttribute : metaStateEvent.getMetaStreamEvents()[0].getOnAfterWindowData()) {
                metaStateEvent.getMetaStreamEvents()[0].addOutputData(outputAttribute);
            }
        }

        if (metaStateEvent.getOutputDataAttributes().size() > 0) {
            while (metaStateEvent.getMetaStreamEvent(0).getOutputData().size() > 0) {
                metaStateEvent.getMetaStreamEvent(0).getOutputData().remove(0);
            }
        }

        QuerySelector querySelector = SelectorParser.parse(selectorForTestOnDemandQuery,
                returnStream,
                metaStateEvent, tableMap,
                variableExpressionExecutorsForQuerySelector, metaPosition, ProcessingMode.BATCH,
                false, siddhiQueryContextForTestOnDemandQuery);
        QueryParserHelper.updateVariablePosition(metaStateEvent,
                variableExpressionExecutorsForQuerySelector);
        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(metaStateEvent));

        compiledSelectionWithCache = new CompiledSelectionWithCache(null, querySelector, metaStateEvent, 0, null);
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

        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>();
        complexEventChunk.add(stateEvent);
        return complexEventChunk;
    }

    private ComplexEventChunk<StateEvent> convForUpdate(List<Map<String, Object>> parameterMapList) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>();
        for (Map<String, Object> parameterMap : parameterMapList) {
            complexEventChunk.add(parameterMapToStateEvent(parameterMap));
        }
        return complexEventChunk;
    }

    private ComplexEventChunk<StreamEvent> objectListToComplexEventChunk(List<Object[]> records) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
        for (Object[] record : records) {
            StreamEvent event = storeEventPool.newInstance();
            event.setOutputData(record);
            complexEventChunk.add(event);
        }
        return complexEventChunk;
    }

    private ComplexEventChunk<StateEvent> convForDelete(List<Map<String, Object>> deleteConditionParameterMaps) {
        List<Object[]> objectList = new LinkedList<>();
        for (Map<String, Object> parameterMap : deleteConditionParameterMaps) {
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

        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>();
        for (Object[] record : objectList) {
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

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }

}

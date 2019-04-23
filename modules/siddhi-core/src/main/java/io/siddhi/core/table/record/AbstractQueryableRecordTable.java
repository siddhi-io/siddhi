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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.SelectorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An abstract implementation of table. Abstract implementation will handle {@link ComplexEventChunk} so that
 * developer can directly work with event data.
 */
public abstract class AbstractQueryableRecordTable extends AbstractRecordTable implements QueryableProcessor {

    //todo: design decision: shall we make a class cachetable extending inmemorytable?

    private static final Logger log = Logger.getLogger(AbstractQueryableRecordTable.class);
    protected int cacheSize;
//    protected TableDefinition tableDefinition;
    protected SiddhiAppContext siddhiAppContext; //todo dont store siddhi app context. Try to get it from connect function
    protected InMemoryTable cachedTable;
    protected Boolean isCacheEnabled = Boolean.FALSE;
    protected Boolean isTableSmallerThanCache = Boolean.FALSE;
    protected TableDefinition cacheTableDefinition;

    @Override
    public void init(TableDefinition tableDefinition, SiddhiAppContext siddhiAppContext,
                     StreamEventCloner storeEventCloner, ConfigReader configReader) {
        this.tableDefinition = tableDefinition;
        this.siddhiAppContext = siddhiAppContext;
        System.out.println("my new init");

        System.out.println("hi from cache");
        List<Annotation> annotationList = tableDefinition.getAnnotations();
        List<Annotation> storeAnnotationList = annotationList.get(0).getAnnotations();
//        String a = storeAnnotationList.get(0).getName();
        for (Annotation b: storeAnnotationList) {
            if (b.getName().equals("Cache")){
                isCacheEnabled = Boolean.TRUE;
                cacheSize = Integer.parseInt(b.getElements().get(0).getValue());
            }
        }

        if (isCacheEnabled) {
            System.out.println("cache is enabled");

//            storeAnnotationList.removeIf(annotation -> annotation.getName().equals("Cache"));
            cachedTable = new InMemoryTable();
            String defineCache = generateCacheTableDefinitionString(tableDefinition);
            cacheTableDefinition = SiddhiCompiler.parseTableDefinition(defineCache);
            cachedTable.initTable(cacheTableDefinition, storeEventPool,
                    storeEventCloner, configReader, siddhiAppContext, recordTableHandler);

//            siddhiManager = new SiddhiManager();
//            String streams = "";
//            String query =  "from TriggerStream#rdbms:query('SAMPLE_DB', 'select * from " +
//                    "Transactions_Table', 'creditcardno string, country string, transaction string," +
//                    " amount int') \n" +
//                    "select creditcardno, country, transaction, amount \n" +
//                    "insert into recordStream;";
//            Expression compareCondition = Expression.value(true);
//            MatchingMetaInfoHolder matchingMetaInfoHolder =
//            ExpressionBuilder myExpressionBuilder = new ExpressionBuilder(compareCondition, matchingMetaInfoHolder,
//                    siddhiAppContext, variableExpressionExecutors,
//                    tableMap, queryName);

        }

//        if tableDefinition.
//        Boolean isCacheEnabled =

    }


    protected String generateCacheTableDefinitionString (TableDefinition tableDefinition) {
        String defineCache = "define table ";
        defineCache = defineCache + tableDefinition.getId() + " (";

        for (Attribute attribute: tableDefinition.getAttributeList()) {
            defineCache = defineCache + attribute.getName() + " " + attribute.getType().name().toLowerCase() + ", ";
        }
        defineCache = defineCache.substring(0, defineCache.length() - 2);
        defineCache = defineCache + "); ";

        return defineCache;
    }

    @Override
    protected void connect(Map<String, Table> tableMap) throws ConnectionUnavailableException {
        connect();

        if (isCacheEnabled) {
            String queryName = "store_select_query_" + tableDefinition.getId();
            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);

            Expression onCondition = Expression.value(true);

            MetaStateEvent metaStateEvent = new MetaStateEvent(1);
            MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
            StoreQuery storeQuery = SiddhiCompiler.parseStoreQuery("from " +
                    tableDefinition.getId()
                    + " select * limit " + (cacheSize + 1));

            //        InputStore inputStore = storeQuery.getInputStore();
            initMetaStreamEvent(metaStreamEvent, tableDefinition);
            metaStateEvent.addEvent(metaStreamEvent);
            //        metaStateEvent.addEvent(metaStreamEvent);
            MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(metaStateEvent,
                    -1, 0, tableDefinition, tableDefinition, 0);
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
//            String queryName = "store_select_query_" + tableDefinition.getId();

            CompiledCondition compiledCondition = compileCondition(onCondition, matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);

//            StateEvent matchingEvent = new StateEvent(1, 0);

            int metaPosition = SiddhiConstants.UNKNOWN_STATE;
            List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(storeQuery, siddhiQueryContext,
                    tableMap, metaPosition, matchingMetaInfoHolder);
            MatchingMetaInfoHolder matchingMetaInfoHolderForSelection = new MatchingMetaInfoHolder(metaStateEvent,
                    -1, 0, generateTableDefinitionFromStoreQuery(storeQuery, expectedOutputAttributes), tableDefinition, 0);


            CompiledSelection compiledSelection = compileSelection(storeQuery.getSelector(), expectedOutputAttributes,
                    matchingMetaInfoHolderForSelection, variableExpressionExecutors, tableMap, siddhiQueryContext);


            Attribute[] outputAttributes = expectedOutputAttributes.toArray(new Attribute[expectedOutputAttributes.size()]);

            StateEvent stateEvent = new StateEvent(1, 0);
            StreamEvent myEvent = query(stateEvent, compiledCondition, compiledSelection, outputAttributes);
            System.out.println("available records loaded");

            int myEventSize = 0;
            if (myEvent != null) {
                myEventSize = 1;
            }
            StreamEvent myEventCopy = myEvent;

            while (myEventCopy.getNext() != null) {
                myEventSize = myEventSize + 1;
                myEventCopy = myEventCopy.getNext();
            }

            if (myEventSize <= cacheSize) {
                isTableSmallerThanCache = Boolean.TRUE;
                ComplexEventChunk<StreamEvent> loadedCache = new ComplexEventChunk<>();
                loadedCache.add(myEvent);

                cachedTable.addEvents(loadedCache, myEventSize);
                System.out.println("cached in cache table");
            }
        }
    }

    protected CompiledCondition generateCacheCompileCondition(Expression condition,
                                                              MatchingMetaInfoHolder storeMatchingMetaInfoHolder,
                                                              SiddhiQueryContext siddhiQueryContext,
                                                              List<VariableExpressionExecutor>
                                                                      storeVariableExpressionExecutors,
                                                              Map<String, Table> storeTableMap) {
        if (isCacheEnabled) {
//            MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
//            metaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
//            initMetaStreamEvent(metaStreamEvent, cacheTableDefinition);
//            MetaStateEvent metaStateEvent = new MetaStateEvent(1);
//            metaStateEvent.addEvent(storeMatchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0));
//            metaStateEvent.addEvent(metaStreamEvent);
//            for (MetaStreamEvent referenceMetaStreamEvent: storeMatchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvents()) {
//
//
//                MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
////            MetaStreamEvent referenceMetaStreamEvent = storeMatchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0);
//                metaStreamEvent.setOutputDefinition(referenceMetaStreamEvent.getOutputStreamDefinition());
//                metaStreamEvent.setInputReferenceId(referenceMetaStreamEvent.getInputReferenceId());
//
//                for (AbstractDefinition inputDefinition : referenceMetaStreamEvent.getInputDefinitions()) {
//                    metaStreamEvent.addInputDefinition(inputDefinition);
//                }
//
//                for (Attribute attribute : referenceMetaStreamEvent.getBeforeWindowData()) {
//                    metaStreamEvent.addOutputData(attribute);
//                }
//
//                metaStreamEvent.initializeAfterWindowData();
//
//                for (Attribute attribute : referenceMetaStreamEvent.getOnAfterWindowData()) {
//                    metaStreamEvent.addOutputData(attribute);
//                }
//
//                metaStateEvent.addEvent(metaStreamEvent);
//            }
//            MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(
//                    metaStateEvent,
//                    storeMatchingMetaInfoHolder.getMatchingStreamEventIndex(),
//                    storeMatchingMetaInfoHolder.getStoreEventIndex(),
//                    storeMatchingMetaInfoHolder.getMatchingStreamDefinition(),
//                    cacheTableDefinition,
//                    storeMatchingMetaInfoHolder.getCurrentState());
//            metaStateEvent.getMetaStreamEvent(0).setEventType(MetaStreamEvent.EventType.TABLE);

            //start of temp
            MetaStateEvent metaStateEvent = new MetaStateEvent(1);
            metaStateEvent.addEvent(storeMatchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0));
            MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(metaStateEvent,
                    -1, 0, cacheTableDefinition, cacheTableDefinition, 0);
            //end of temp
            Map<String, Table> tableMap = new ConcurrentHashMap<>();
            tableMap.put(cacheTableDefinition.getId(), cachedTable);
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();

            return cachedTable.compileCondition(condition, matchingMetaInfoHolder,
                    variableExpressionExecutors, tableMap, siddhiQueryContext);
        } else {
            return null;
        }
    }

    private static void initMetaStreamEvent(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition) {//duplicated from org.wso2.siddhi.core.util.parser.StoreQueryParser
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.initializeAfterWindowData();
        inputDefinition.getAttributeList().forEach(metaStreamEvent::addData);
    }

    private static AbstractDefinition generateTableDefinitionFromStoreQuery(StoreQuery storeQuery, //duplicated from org.wso2.siddhi.core.util.parser.StoreQueryParser
                                                                            List<Attribute> expectedOutputAttributes) {
        TableDefinition tableDefinition = TableDefinition.id(storeQuery.getInputStore().getStoreId());
        for (Attribute attribute: expectedOutputAttributes) {
            tableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        return tableDefinition;
    }
    private static List<Attribute> buildExpectedOutputAttributes( //duplicated from org.wso2.siddhi.core.util.parser.StoreQueryParser
                                                                  StoreQuery storeQuery, SiddhiQueryContext siddhiQueryContext, Map<String, Table> tableMap,
                                                                  int metaPosition, MatchingMetaInfoHolder metaStreamInfoHolder) {
//        MetaStateEvent selectMetaStateEvent =
//                new MetaStateEvent(metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvents());
//        SelectorParser.parse(storeQuery.getSelector(),
//                new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS), siddhiAppContext,
//                selectMetaStateEvent, tableMap, new ArrayList<>(), queryName,
//                metaPosition);

        MetaStateEvent selectMetaStateEvent =
                new MetaStateEvent(metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvents());
        SelectorParser.parse(storeQuery.getSelector(),
                new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS),
                selectMetaStateEvent, tableMap, new ArrayList<>(), metaPosition, ProcessingMode.BATCH,
                false, siddhiQueryContext);

        return selectMetaStateEvent.getOutputStreamDefinition().getAttributeList();
    }

    protected abstract void connect() throws ConnectionUnavailableException;

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection) throws ConnectionUnavailableException {
        return query(matchingEvent, compiledCondition, compiledSelection, null);
    }

    public StreamEvent findInCache(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        if (isCacheEnabled && !cachedTable.isEmpty()) {
            StreamEvent cacheResults = cachedTable.find(matchingEvent, compiledCondition);
//            if (cacheResults != null) {
////                executeSelector(,cacheResults, );
//            }
            return cacheResults;
        } else {
            return null;
        }

    }

    private Event[] executeSelector(StreamEvent streamEvents, QuerySelector selector, MatchingMetaInfoHolder metaStreamInfoHolder) {
        ComplexEventChunk<StateEvent> complexEventChunk = new ComplexEventChunk<>(true);
        MetaStreamEvent.EventType eventType = MetaStreamEvent.EventType.TABLE;
        while (streamEvents != null) {

            StreamEvent streamEvent = streamEvents;
            streamEvents = streamEvents.getNext();
            streamEvent.setNext(null);

            StateEventFactory stateEventFactory = new StateEventFactory(metaStreamInfoHolder.getMetaStateEvent());

            StateEvent stateEvent = stateEventFactory.newInstance();
            if (eventType == MetaStreamEvent.EventType.AGGREGATE) {
                stateEvent.addEvent(1, streamEvent);
            } else {
                stateEvent.addEvent(0, streamEvent);
            }
            complexEventChunk.add(stateEvent);
        }
        ComplexEventChunk outputComplexEventChunk = selector.execute(complexEventChunk);
        if (outputComplexEventChunk != null) {
            List<Event> events = new ArrayList<>();
            outputComplexEventChunk.reset();
            while (outputComplexEventChunk.hasNext()) {
                ComplexEvent complexEvent = outputComplexEventChunk.next();
                events.add(new Event(complexEvent.getTimestamp(), complexEvent.getOutputData()));
            }
            return events.toArray(new Event[0]);
        } else {
            return null;
        }

    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelectionTemp, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {

        RecordStoreCompiledCondition compiledConditionTemp = (RecordStoreCompiledCondition) compiledCondition;
        CompiledConditionAggregation compiledConditionAggregation = (CompiledConditionAggregation) compiledConditionTemp.compiledCondition;

        StreamEvent cacheResults = findInCache(compiledConditionAggregation.getCacheCompileCondition(), matchingEvent);
        ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>(true);

        CompiledSelectionAggregation compiledSelectionAggregation = (CompiledSelectionAggregation) compiledSelectionTemp;
        CompiledSelection compiledSelection = compiledSelectionAggregation.recordStoreCompiledSelection;

        if (cacheResults != null) {
            System.out.println("sending results from cache");
            Event[] cacheResultsAfterSelection = executeSelector(cacheResults, compiledSelectionAggregation.querySelector,
                    compiledSelectionAggregation.metaStreamInfoHolder);
            for (Event event: cacheResultsAfterSelection) {

                Object[] record = event.getData();
                StreamEvent streamEvent = storeEventPool.newInstance();
                streamEvent.setOutputData(new Object[outputAttributes.length]);
                System.arraycopy(record, 0, streamEvent.getOutputData(), 0, record.length);
                streamEventComplexEventChunk.add(streamEvent);
            }
//            streamEventComplexEventChunk.add(cacheResultsAfterSelection);
        } else {

            RecordStoreCompiledSelection recordStoreCompiledSelection = ((RecordStoreCompiledSelection) compiledSelection);
            RecordStoreCompiledCondition recordStoreCompiledCondition = new RecordStoreCompiledCondition(compiledConditionTemp.variableExpressionExecutorMap, compiledConditionAggregation.getStoreCompileCondition());;

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
//            ComplexEventChunk<StreamEvent> streamEventComplexEventChunk = new ComplexEventChunk<>(true);
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

    public class CompiledSelectionAggregation implements CompiledSelection{

        RecordStoreCompiledSelection recordStoreCompiledSelection;

        public QuerySelector getQuerySelector() {
            return querySelector;
        }

        QuerySelector querySelector;
        MatchingMetaInfoHolder metaStreamInfoHolder;



        public CompiledSelectionAggregation(RecordStoreCompiledSelection recordStoreCompiledSelection, QuerySelector querySelector,
                                            MatchingMetaInfoHolder metaStreamInfoHolder) {
            this.recordStoreCompiledSelection = recordStoreCompiledSelection;
            this.querySelector = querySelector;
            this.metaStreamInfoHolder = metaStreamInfoHolder;
        }

//        @Override
//        public CompiledSelection cloneCompilation(String key) {
//            return null;
//        }
    }



    public CompiledSelection compileSelection(Selector selector,
                                              List<Attribute> expectedOutputAttributes,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        CompiledSelectionAggregation compiledSelectionAggregation;

        ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
        int metaPosition = SiddhiConstants.UNKNOWN_STATE;
        List<VariableExpressionExecutor> variableExpressionExecutorsForQuerySelector = new ArrayList<>();
//        MetaStateEvent metaStateEvent = new MetaStateEvent(1);
//        metaStateEvent.addEvent(matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0));
//        MatchingMetaInfoHolder matchingMetaInfoHolderforSelector = new MatchingMetaInfoHolder(
//                metaStateEvent, -1, 0, cacheTableDefinition, cacheTableDefinition, 0);
//        metaStateEvent.getMetaStreamEvent(0).setEventType(MetaStreamEvent.EventType.TABLE);

        QuerySelector querySelector = SelectorParser.parse(selector,
                returnStream,
                matchingMetaInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutorsForQuerySelector,
                metaPosition, ProcessingMode.BATCH, false, siddhiQueryContext);

//        QueryParserHelper.reduceMetaComplexEvent(metaStateEvent);
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(), variableExpressionExecutorsForQuerySelector);

        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(matchingMetaInfoHolder.getMetaStateEvent()));

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
        RecordStoreCompiledSelection recordStoreCompiledSelection = new RecordStoreCompiledSelection(expressionExecutorMap, compiledSelection);

        compiledSelectionAggregation = new CompiledSelectionAggregation(recordStoreCompiledSelection, querySelector, matchingMetaInfoHolder);
        return compiledSelectionAggregation;
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

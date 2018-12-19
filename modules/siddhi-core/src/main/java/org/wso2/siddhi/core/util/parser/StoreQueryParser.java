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

package org.wso2.siddhi.core.util.parser;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.aggregation.AggregationRuntime;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.StateEventPool;
import org.wso2.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent.EventType;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import org.wso2.siddhi.core.exception.QueryableRecordTableException;
import org.wso2.siddhi.core.exception.StoreQueryCreationException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.DeleteStoreQueryRuntime;
import org.wso2.siddhi.core.query.FindStoreQueryRuntime;
import org.wso2.siddhi.core.query.InsertStoreQueryRuntime;
import org.wso2.siddhi.core.query.SelectStoreQueryRuntime;
import org.wso2.siddhi.core.query.StoreQueryRuntime;
import org.wso2.siddhi.core.query.UpdateOrInsertStoreQueryRuntime;
import org.wso2.siddhi.core.query.UpdateStoreQueryRuntime;
import org.wso2.siddhi.core.query.output.callback.OutputCallback;
import org.wso2.siddhi.core.query.output.ratelimit.PassThroughOutputRateLimiter;
import org.wso2.siddhi.core.query.processor.stream.window.QueryableProcessor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;
import org.wso2.siddhi.core.util.collection.operator.IncrementalAggregateCompileCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;
import org.wso2.siddhi.core.window.Window;
import org.wso2.siddhi.query.api.aggregation.Within;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.execution.query.StoreQuery;
import org.wso2.siddhi.query.api.execution.query.input.store.AggregationInputStore;
import org.wso2.siddhi.query.api.execution.query.input.store.ConditionInputStore;
import org.wso2.siddhi.query.api.execution.query.input.store.InputStore;
import org.wso2.siddhi.query.api.execution.query.output.stream.DeleteStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.OutputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.ReturnStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.UpdateOrInsertStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.UpdateStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class to parse {@link StoreQueryRuntime}.
 */
public class StoreQueryParser {

    /**
     * Parse a storeQuery and return corresponding StoreQueryRuntime.
     *
     * @param storeQuery       storeQuery to be parsed.
     * @param siddhiAppContext associated Siddhi app context.
     * @param tableMap         keyvalue containing tables.
     * @param windowMap        keyvalue containing windows.
     * @param aggregationMap   keyvalue containing aggregation runtimes.
     * @return StoreQueryRuntime
     */
    private static final Logger log = Logger.getLogger(StoreQueryParser.class);

    public static StoreQueryRuntime parse(StoreQuery storeQuery, SiddhiAppContext siddhiAppContext,
                                          Map<String, Table> tableMap, Map<String, Window> windowMap,
                                          Map<String, AggregationRuntime> aggregationMap) {

        final LockWrapper lockWrapper = new LockWrapper("StoreQueryLock");
        lockWrapper.setLock(new ReentrantLock());

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();

        int metaPosition = SiddhiConstants.UNKNOWN_STATE;
        String queryName;
        Table table;
        Expression onCondition;

        SnapshotService.getSkipSnapshotableThreadLocal().set(true);

        switch (storeQuery.getType()) {
            case FIND:
                Within within = null;
                Expression per = null;
                queryName = "store_select_query_" + storeQuery.getInputStore().getStoreId();
                InputStore inputStore = storeQuery.getInputStore();
                try {
                    onCondition = Expression.value(true);
                    metaStreamEvent.setInputReferenceId(inputStore.getStoreReferenceId());

                    if (inputStore instanceof AggregationInputStore) {
                        AggregationInputStore aggregationInputStore = (AggregationInputStore) inputStore;
                        if (aggregationMap.get(inputStore.getStoreId()) == null) {
                            throw new StoreQueryCreationException("Aggregation \"" + inputStore.getStoreId() +
                                    "\" has not been defined");
                        }
                        if (aggregationInputStore.getPer() != null && aggregationInputStore.getWithin() != null) {
                            within = aggregationInputStore.getWithin();
                            per = aggregationInputStore.getPer();
                        } else if (aggregationInputStore.getPer() != null ||
                                aggregationInputStore.getWithin() != null) {
                                    throw new StoreQueryCreationException(
                                            inputStore.getStoreId() + " should either have both 'within' and 'per' " +
                                                    "defined or none.");
                        }
                        if (((AggregationInputStore) inputStore).getOnCondition() != null) {
                            onCondition = ((AggregationInputStore) inputStore).getOnCondition();
                        }
                    } else if (inputStore instanceof ConditionInputStore) {
                        if (((ConditionInputStore) inputStore).getOnCondition() != null) {
                            onCondition = ((ConditionInputStore) inputStore).getOnCondition();
                        }
                    }
                    List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
                    table = tableMap.get(inputStore.getStoreId());
                    if (table != null) {
                        return constructStoreQueryRuntime(table, storeQuery, siddhiAppContext, tableMap, windowMap,
                                queryName,
                                metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors, lockWrapper);
                    } else {
                        AggregationRuntime aggregation = aggregationMap.get(inputStore.getStoreId());
                        if (aggregation != null) {
                            return constructStoreQueryRuntime(aggregation, storeQuery, siddhiAppContext, tableMap,
                                    windowMap, queryName, within, per, onCondition, metaStreamEvent,
                                    variableExpressionExecutors, lockWrapper);
                        } else {
                            Window window = windowMap.get(inputStore.getStoreId());
                            if (window != null) {
                                return constructStoreQueryRuntime(window, storeQuery, siddhiAppContext,
                                        tableMap, windowMap, queryName, metaPosition, onCondition, metaStreamEvent,
                                        variableExpressionExecutors, lockWrapper);
                            } else {
                                throw new StoreQueryCreationException(
                                        inputStore.getStoreId() + " is neither a table, aggregation or window");
                            }
                        }
                    }

                } finally {
                    SnapshotService.getSkipSnapshotableThreadLocal().set(null);
                }
            case INSERT:
                InsertIntoStream inserIntoStreamt = (InsertIntoStream) storeQuery.getOutputStream();
                queryName = "store_insert_query_" + inserIntoStreamt.getId();
                onCondition = Expression.value(true);

                return getStoreQueryRuntime(storeQuery, siddhiAppContext, tableMap, windowMap, queryName, metaPosition,
                        lockWrapper, metaStreamEvent, inserIntoStreamt, onCondition);
            case DELETE:
                DeleteStream deleteStream = (DeleteStream) storeQuery.getOutputStream();
                queryName = "store_delete_query_" + deleteStream.getId();
                onCondition = deleteStream.getOnDeleteExpression();

                return getStoreQueryRuntime(storeQuery, siddhiAppContext, tableMap, windowMap, queryName, metaPosition,
                        lockWrapper, metaStreamEvent, deleteStream, onCondition);
            case UPDATE:
                UpdateStream outputStream = (UpdateStream) storeQuery.getOutputStream();
                queryName = "store_update_query_" + outputStream.getId();
                onCondition = outputStream.getOnUpdateExpression();

                return getStoreQueryRuntime(storeQuery, siddhiAppContext, tableMap, windowMap, queryName, metaPosition,
                        lockWrapper, metaStreamEvent, outputStream, onCondition);
            case UPDATE_OR_INSERT:
                UpdateOrInsertStream storeQueryOutputStream = (UpdateOrInsertStream) storeQuery.getOutputStream();
                queryName = "store_update_or_insert_query_" + storeQueryOutputStream.getId();
                onCondition = storeQueryOutputStream.getOnUpdateExpression();

                return getStoreQueryRuntime(storeQuery, siddhiAppContext, tableMap, windowMap, queryName, metaPosition,
                        lockWrapper, metaStreamEvent, storeQueryOutputStream, onCondition);
            default:
                return null;
        }
    }

    private static StoreQueryRuntime getStoreQueryRuntime(StoreQuery storeQuery, SiddhiAppContext siddhiAppContext,
                                                          Map<String, Table> tableMap, Map<String, Window> windowMap,
                                                          String queryName, int metaPosition, LockWrapper lockWrapper,
                                                          MetaStreamEvent metaStreamEvent,
                                                          OutputStream outputStream, Expression onCondition) {
        try {
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
            Table table = tableMap.get(outputStream.getId());

            if (table != null) {
                return constructStoreQueryRuntime(table, storeQuery, siddhiAppContext, tableMap, windowMap,
                        queryName, metaPosition, onCondition, metaStreamEvent,
                        variableExpressionExecutors, lockWrapper);
            } else {
                throw new StoreQueryCreationException(outputStream.getId() + " is not a table.");
            }

        } finally {
            SnapshotService.getSkipSnapshotableThreadLocal().set(null);
        }
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(
            Window window, StoreQuery storeQuery,
            SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap, Map<String, Window> windowMap,
            String queryName, int metaPosition, Expression onCondition, MetaStreamEvent metaStreamEvent,
            List<VariableExpressionExecutor> variableExpressionExecutors, LockWrapper lockWrapper) {
        metaStreamEvent.setEventType(EventType.WINDOW);
        initMetaStreamEvent(metaStreamEvent, window.getWindowDefinition());
        MatchingMetaInfoHolder metaStreamInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                window.getWindowDefinition());
        CompiledCondition compiledCondition = window.compileCondition(onCondition,
                generateMatchingMetaInfoHolder(metaStreamEvent, window.getWindowDefinition()),
                siddhiAppContext, variableExpressionExecutors, tableMap, queryName);
        FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(window, compiledCondition,
                queryName, metaStreamEvent);
        populateFindStoreQueryRuntime(findStoreQueryRuntime, metaStreamInfoHolder, storeQuery.getSelector(),
                variableExpressionExecutors, siddhiAppContext, tableMap, windowMap, queryName, metaPosition,
                lockWrapper);
        return findStoreQueryRuntime;
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(AggregationRuntime aggregation, StoreQuery storeQuery,
                                                                SiddhiAppContext siddhiAppContext,
                                                                Map<String, Table> tableMap,
                                                                Map<String, Window>  windowMap, String queryName,
                                                                Within within, Expression per, Expression onCondition,
                                                                MetaStreamEvent metaStreamEvent,
                                                                List<VariableExpressionExecutor>
                                                                        variableExpressionExecutors,
                                                                LockWrapper lockWrapper) {
        int metaPosition;
        metaStreamEvent.setEventType(EventType.AGGREGATE);
        initMetaStreamEvent(metaStreamEvent, aggregation.getAggregationDefinition());
        MatchingMetaInfoHolder metaStreamInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                aggregation.getAggregationDefinition());
        CompiledCondition compiledCondition = aggregation.compileExpression(onCondition, within, per,
                metaStreamInfoHolder, variableExpressionExecutors, tableMap, queryName, siddhiAppContext);
        metaStreamInfoHolder = ((IncrementalAggregateCompileCondition) compiledCondition).
                getAlteredMatchingMetaInfoHolder();
        FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(aggregation, compiledCondition,
                queryName, metaStreamEvent);
        metaPosition = 1;
        populateFindStoreQueryRuntime(findStoreQueryRuntime, metaStreamInfoHolder,
                storeQuery.getSelector(), variableExpressionExecutors, siddhiAppContext, tableMap, windowMap,
                queryName, metaPosition, lockWrapper);
        ComplexEventPopulater complexEventPopulater = StreamEventPopulaterFactory.constructEventPopulator(
                metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvent(0), 0,
                ((IncrementalAggregateCompileCondition) compiledCondition).getAdditionalAttributes());
        ((IncrementalAggregateCompileCondition) compiledCondition)
                .setComplexEventPopulater(complexEventPopulater);
        return findStoreQueryRuntime;
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(Table table, StoreQuery storeQuery,
                                                             SiddhiAppContext siddhiAppContext,
                                           Map<String, Table> tableMap, Map<String, Window> windowMap,
                                           String queryName, int metaPosition, Expression onCondition,
                                           MetaStreamEvent metaStreamEvent,
                                           List<VariableExpressionExecutor> variableExpressionExecutors,
                                           LockWrapper lockWrapper) {
        if (table instanceof QueryableProcessor && storeQuery.getType() == StoreQuery.StoreQueryType.FIND) {
            try {
                return constructOptimizedStoreQueryRuntime(table, storeQuery, siddhiAppContext, tableMap,
                        queryName, metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors);
            } catch (QueryableRecordTableException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Store Query optimization failed for table: "
                            + table.getTableDefinition().getId() + ". Creating Store Query runtime in normal mode. " +
                            "Reason for failure: " + e.getMessage());
                } else {
                    log.info("Creating Store Query Runtime in the normal mode, for table: "
                            + table.getTableDefinition().getId());
                }
                return constructRegularStoreQueryRuntime(table, storeQuery, siddhiAppContext, tableMap, windowMap,
                        queryName, metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                        lockWrapper);
            }
        } else {
            return constructRegularStoreQueryRuntime(table, storeQuery, siddhiAppContext, tableMap, windowMap,
                    queryName, metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                    lockWrapper);
        }
    }

    private static StoreQueryRuntime constructOptimizedStoreQueryRuntime(Table table,
                                                                         StoreQuery storeQuery,
                                                                         SiddhiAppContext siddhiAppContext,
                                                                         Map<String, Table> tableMap, String queryName,
                                                                         int metaPosition, Expression onCondition,
                                                                         MetaStreamEvent metaStreamEvent,
                                                                         List<VariableExpressionExecutor>
                                                                        variableExpressionExecutors) {
        MatchingMetaInfoHolder matchingMetaInfoHolder;

        initMetaStreamEvent(metaStreamEvent, table.getTableDefinition());
        matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, table.getTableDefinition());
        CompiledCondition compiledCondition = table.compileCondition(onCondition, matchingMetaInfoHolder,
                siddhiAppContext, variableExpressionExecutors, tableMap, queryName);
        List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(storeQuery, siddhiAppContext,
                tableMap, queryName, metaPosition, matchingMetaInfoHolder);

        MatchingMetaInfoHolder matchingMetaInfoHolderForSelection = generateMatchingMetaInfoHolder(
                metaStreamEvent, generateTableDefinitionFromStoreQuery(storeQuery, expectedOutputAttributes),
                table.getTableDefinition());
        CompiledSelection compiledSelection = ((QueryableProcessor) table).compileSelection(
                storeQuery.getSelector(), expectedOutputAttributes, matchingMetaInfoHolderForSelection,
                siddhiAppContext, variableExpressionExecutors, tableMap, queryName);

        StoreQueryRuntime storeQueryRuntime =
                new SelectStoreQueryRuntime((QueryableProcessor) table, compiledCondition,
                        compiledSelection, expectedOutputAttributes, queryName);
        QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                variableExpressionExecutors);
        return storeQueryRuntime;
    }

    private static StoreQueryRuntime constructRegularStoreQueryRuntime(Table table, StoreQuery storeQuery,
                                                                       SiddhiAppContext siddhiAppContext,
                                                                       Map<String, Table> tableMap, Map<String, Window>
                                                                        windowMap, String
                                                                        queryName,
                                                                       int metaPosition, Expression onCondition,
                                                                       MetaStreamEvent metaStreamEvent,
                                                                       List<VariableExpressionExecutor>
                                                                        variableExpressionExecutors,
                                                                       LockWrapper lockWrapper) {
        MatchingMetaInfoHolder matchingMetaInfoHolder;
        AbstractDefinition inputDefinition;
        QuerySelector querySelector;
        metaStreamEvent.setEventType(EventType.TABLE);

            switch (storeQuery.getType()) {
                case FIND:
                    initMetaStreamEvent(metaStreamEvent, table.getTableDefinition());
                    matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                            table.getTableDefinition());
                    CompiledCondition compiledCondition = table.compileCondition(onCondition, matchingMetaInfoHolder,
                            siddhiAppContext, variableExpressionExecutors, tableMap, queryName);

                    FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(table, compiledCondition,
                            queryName, metaStreamEvent);
                    populateFindStoreQueryRuntime(findStoreQueryRuntime, matchingMetaInfoHolder,
                            storeQuery.getSelector(), variableExpressionExecutors, siddhiAppContext,
                            tableMap, windowMap, queryName, metaPosition, lockWrapper);
                    return findStoreQueryRuntime;
                case INSERT:
                    initMetaStreamEvent(metaStreamEvent, getInputDefinition(storeQuery, table));
                    matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                            table.getTableDefinition());
                    querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                            siddhiAppContext, tableMap, windowMap, queryName, metaPosition, storeQuery, lockWrapper);

                    InsertStoreQueryRuntime insertStoreQueryRuntime =
                            new InsertStoreQueryRuntime(queryName, metaStreamEvent);
                    insertStoreQueryRuntime.setStateEventPool(
                            new StateEventPool(matchingMetaInfoHolder.getMetaStateEvent(), 5));
                    insertStoreQueryRuntime.setSelector(querySelector);
                    insertStoreQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                            .getOutputStreamDefinition().getAttributeList());
                    return insertStoreQueryRuntime;
                case DELETE:
                    inputDefinition = getInputDefinition(storeQuery, table);
                    initMetaStreamEvent(metaStreamEvent, inputDefinition);
                    matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                            inputDefinition, table.getTableDefinition());
                    querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                            siddhiAppContext, tableMap, windowMap, queryName, metaPosition, storeQuery, lockWrapper);

                    DeleteStoreQueryRuntime deleteStoreQueryRuntime =
                            new DeleteStoreQueryRuntime(queryName, metaStreamEvent);
                    deleteStoreQueryRuntime.setStateEventPool(
                            new StateEventPool(matchingMetaInfoHolder.getMetaStateEvent(), 5));
                    deleteStoreQueryRuntime.setSelector(querySelector);
                    deleteStoreQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                            .getOutputStreamDefinition().getAttributeList());
                    return deleteStoreQueryRuntime;
                case UPDATE:
                    inputDefinition = getInputDefinition(storeQuery, table);
                    initMetaStreamEvent(metaStreamEvent, inputDefinition);
                    matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, inputDefinition,
                            table.getTableDefinition());
                    querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                            siddhiAppContext, tableMap, windowMap, queryName, metaPosition, storeQuery, lockWrapper);

                    UpdateStoreQueryRuntime updateStoreQueryRuntime =
                            new UpdateStoreQueryRuntime(queryName, metaStreamEvent);
                    updateStoreQueryRuntime.setStateEventPool(
                            new StateEventPool(matchingMetaInfoHolder.getMetaStateEvent(), 5));
                    updateStoreQueryRuntime.setSelector(querySelector);
                    updateStoreQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                            .getOutputStreamDefinition().getAttributeList());
                    return updateStoreQueryRuntime;
                case UPDATE_OR_INSERT:
                    inputDefinition = getInputDefinition(storeQuery, table);
                    initMetaStreamEvent(metaStreamEvent, inputDefinition);
                    matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, inputDefinition,
                            table.getTableDefinition());
                    querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                            siddhiAppContext, tableMap, windowMap, queryName, metaPosition, storeQuery, lockWrapper);

                    UpdateOrInsertStoreQueryRuntime updateOrInsertIntoStoreQueryRuntime =
                            new UpdateOrInsertStoreQueryRuntime(queryName, metaStreamEvent);
                    updateOrInsertIntoStoreQueryRuntime.setStateEventPool(
                            new StateEventPool(matchingMetaInfoHolder.getMetaStateEvent(), 5));
                    updateOrInsertIntoStoreQueryRuntime.setSelector(querySelector);
                    updateOrInsertIntoStoreQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                            .getOutputStreamDefinition().getAttributeList());
                    return updateOrInsertIntoStoreQueryRuntime;
                default:
                    return null;
            }
    }

    private static List<Attribute> buildExpectedOutputAttributes(
            StoreQuery storeQuery, SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap,
            String queryName, int metaPosition, MatchingMetaInfoHolder metaStreamInfoHolder) {
        MetaStateEvent selectMetaStateEvent =
                new MetaStateEvent(metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvents());
        SelectorParser.parse(storeQuery.getSelector(),
                new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS), siddhiAppContext,
                selectMetaStateEvent, tableMap, new ArrayList<>(), queryName,
                metaPosition);
        return selectMetaStateEvent.getOutputStreamDefinition().getAttributeList();
    }

    private static void populateFindStoreQueryRuntime(FindStoreQueryRuntime findStoreQueryRuntime,
                                                      MatchingMetaInfoHolder metaStreamInfoHolder, Selector selector,
                                                      List<VariableExpressionExecutor> variableExpressionExecutors,
                                                      SiddhiAppContext siddhiAppContext,
                                                      Map<String, Table> tableMap, Map<String, Window> windowMap,
                                                      String queryName, int metaPosition, LockWrapper lockWrapper) {
        ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
        QuerySelector querySelector = SelectorParser.parse(selector,
                returnStream, siddhiAppContext,
                metaStreamInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutors, queryName,
                metaPosition);
        PassThroughOutputRateLimiter rateLimiter = new PassThroughOutputRateLimiter(queryName);
        rateLimiter.init(siddhiAppContext, lockWrapper, queryName);
        OutputCallback outputCallback = OutputParser.constructOutputCallback(returnStream,
                metaStreamInfoHolder.getMetaStateEvent().getOutputStreamDefinition(), tableMap, windowMap,
                siddhiAppContext, true, queryName);
        rateLimiter.setOutputCallback(outputCallback);
        querySelector.setNextProcessor(rateLimiter);

        QueryParserHelper.reduceMetaComplexEvent(metaStreamInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(metaStreamInfoHolder.getMetaStateEvent(), variableExpressionExecutors);
        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(metaStreamInfoHolder.getMetaStateEvent()));
        findStoreQueryRuntime.setStateEventPool(new StateEventPool(metaStreamInfoHolder.getMetaStateEvent(), 5));
        findStoreQueryRuntime.setSelector(querySelector);
        findStoreQueryRuntime.setOutputAttributes(metaStreamInfoHolder.getMetaStateEvent().
                getOutputStreamDefinition().getAttributeList());
    }

    private static QuerySelector getQuerySelector(MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                                  SiddhiAppContext siddhiAppContext, Map<String, Table> tableMap,
                                                  Map<String, Window> windowMap, String queryName, int metaPosition,
                                                  StoreQuery storeQuery, LockWrapper lockWrapper) {
        QuerySelector querySelector = SelectorParser.parse(storeQuery.getSelector(),
                storeQuery.getOutputStream(), siddhiAppContext, matchingMetaInfoHolder.getMetaStateEvent(),
                tableMap, variableExpressionExecutors, queryName, metaPosition);

        PassThroughOutputRateLimiter rateLimiter = new PassThroughOutputRateLimiter(queryName);
        rateLimiter.init(siddhiAppContext, lockWrapper, queryName);
        OutputCallback outputCallback = OutputParser.constructOutputCallback(storeQuery.getOutputStream(),
                matchingMetaInfoHolder.getMetaStateEvent().getOutputStreamDefinition(), tableMap, windowMap,
                siddhiAppContext, true, queryName);
        rateLimiter.setOutputCallback(outputCallback);
        querySelector.setNextProcessor(rateLimiter);

        QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                variableExpressionExecutors);
        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(matchingMetaInfoHolder.getMetaStateEvent()));
        return querySelector;
    }

    private static MatchingMetaInfoHolder generateMatchingMetaInfoHolder(MetaStreamEvent metaStreamEvent,
                                                                         AbstractDefinition definition) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(1);
        metaStateEvent.addEvent(metaStreamEvent);
        return new MatchingMetaInfoHolder(metaStateEvent, -1, 0, definition,
                definition, 0);
    }

    private static AbstractDefinition generateTableDefinitionFromStoreQuery(StoreQuery storeQuery,
                                                                            List<Attribute> expectedOutputAttributes) {
        TableDefinition tableDefinition = TableDefinition.id(storeQuery.getInputStore().getStoreId());
        for (Attribute attribute: expectedOutputAttributes) {
            tableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        return tableDefinition;
    }

    private static MatchingMetaInfoHolder generateMatchingMetaInfoHolder(MetaStreamEvent metaStreamEvent,
                                                                          AbstractDefinition streamDefinition,
                                                                          AbstractDefinition storeDefinition) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(1);
        metaStateEvent.addEvent(metaStreamEvent);
        return new MatchingMetaInfoHolder(metaStateEvent, -1, 0, streamDefinition,
                storeDefinition, 0);
    }

    private static void initMetaStreamEvent(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition) {
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.initializeAfterWindowData();
        inputDefinition.getAttributeList().forEach(metaStreamEvent::addData);
    }

    private static AbstractDefinition getInputDefinition(StoreQuery storeQuery, Table table) {
        if (storeQuery.getSelector().getSelectionList().isEmpty()) {
            return table.getTableDefinition();
        } else {
            StreamDefinition streamDefinition = new StreamDefinition();
            streamDefinition.setId(table.getTableDefinition().getId() + "InputStream");
            return streamDefinition;
        }
    }
}


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

package io.siddhi.core.util.parser;

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.MetaStreamEvent.EventType;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import io.siddhi.core.exception.QueryableRecordTableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.StoreQueryCreationException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.DeleteStoreQueryRuntime;
import io.siddhi.core.query.FindStoreQueryRuntime;
import io.siddhi.core.query.InsertStoreQueryRuntime;
import io.siddhi.core.query.SelectStoreQueryRuntime;
import io.siddhi.core.query.StoreQueryRuntime;
import io.siddhi.core.query.UpdateOrInsertStoreQueryRuntime;
import io.siddhi.core.query.UpdateStoreQueryRuntime;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.ratelimit.PassThroughOutputRateLimiter;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.IncrementalAggregateCompileCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.execution.query.input.store.AggregationInputStore;
import io.siddhi.query.api.execution.query.input.store.ConditionInputStore;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.output.stream.DeleteStream;
import io.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateOrInsertStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import org.apache.log4j.Logger;

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
     * @param siddhiQueryContext associated Siddhi query context.
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
        SiddhiQueryContext siddhiQueryContext;
        Expression onCondition;

        SnapshotService.getSkipStateStorageThreadLocal().set(true);

        switch (storeQuery.getType()) {
            case FIND:
                Within within = null;
                Expression per = null;
                queryName = "store_select_query_" + storeQuery.getInputStore().getStoreId();
                siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);
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
                        return constructStoreQueryRuntime(table, storeQuery, tableMap, windowMap,
                                metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors, lockWrapper,
                                siddhiQueryContext);
                    } else {
                        AggregationRuntime aggregation = aggregationMap.get(inputStore.getStoreId());
                        if (aggregation != null) {
                            return constructStoreQueryRuntime(aggregation, storeQuery, tableMap,
                                    windowMap, within, per, onCondition, metaStreamEvent,
                                    variableExpressionExecutors, lockWrapper, siddhiQueryContext);
                        } else {
                            Window window = windowMap.get(inputStore.getStoreId());
                            if (window != null) {
                                return constructStoreQueryRuntime(window, storeQuery,
                                        tableMap, windowMap, metaPosition, onCondition, metaStreamEvent,
                                        variableExpressionExecutors, lockWrapper, siddhiQueryContext);
                            } else {
                                throw new StoreQueryCreationException(
                                        inputStore.getStoreId() + " is neither a table, aggregation or window");
                            }
                        }
                    }

                } finally {
                    SnapshotService.getSkipStateStorageThreadLocal().set(null);
                }
            case INSERT:
                InsertIntoStream inserIntoStreamt = (InsertIntoStream) storeQuery.getOutputStream();
                queryName = "store_insert_query_" + inserIntoStreamt.getId();
                siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);
                onCondition = Expression.value(true);

                return getStoreQueryRuntime(storeQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, inserIntoStreamt, onCondition, siddhiQueryContext);
            case DELETE:
                DeleteStream deleteStream = (DeleteStream) storeQuery.getOutputStream();
                queryName = "store_delete_query_" + deleteStream.getId();
                siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);
                onCondition = deleteStream.getOnDeleteExpression();

                return getStoreQueryRuntime(storeQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, deleteStream, onCondition, siddhiQueryContext);
            case UPDATE:
                UpdateStream outputStream = (UpdateStream) storeQuery.getOutputStream();
                queryName = "store_update_query_" + outputStream.getId();
                siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);
                onCondition = outputStream.getOnUpdateExpression();

                return getStoreQueryRuntime(storeQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, outputStream, onCondition, siddhiQueryContext);
            case UPDATE_OR_INSERT:
                UpdateOrInsertStream storeQueryOutputStream = (UpdateOrInsertStream) storeQuery.getOutputStream();
                queryName = "store_update_or_insert_query_" + storeQueryOutputStream.getId();
                siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName);
                onCondition = storeQueryOutputStream.getOnUpdateExpression();

                return getStoreQueryRuntime(storeQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, storeQueryOutputStream, onCondition, siddhiQueryContext);
            default:
                return null;
        }
    }

    private static StoreQueryRuntime getStoreQueryRuntime(StoreQuery storeQuery,
                                                          Map<String, Table> tableMap, Map<String, Window> windowMap,
                                                          int metaPosition, LockWrapper lockWrapper,
                                                          MetaStreamEvent metaStreamEvent,
                                                          OutputStream outputStream, Expression onCondition,
                                                          SiddhiQueryContext siddhiQueryContext) {
        try {
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
            Table table = tableMap.get(outputStream.getId());

            if (table != null) {
                return constructStoreQueryRuntime(table, storeQuery, tableMap, windowMap,
                        metaPosition, onCondition, metaStreamEvent,
                        variableExpressionExecutors, lockWrapper, siddhiQueryContext);
            } else {
                throw new StoreQueryCreationException(outputStream.getId() + " is not a table.");
            }

        } finally {
            SnapshotService.getSkipStateStorageThreadLocal().set(null);
        }
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(
            Window window, StoreQuery storeQuery,
            Map<String, Table> tableMap, Map<String, Window> windowMap,
            int metaPosition, Expression onCondition, MetaStreamEvent metaStreamEvent,
            List<VariableExpressionExecutor> variableExpressionExecutors, LockWrapper lockWrapper,
            SiddhiQueryContext siddhiQueryContext) {
        metaStreamEvent.setEventType(EventType.WINDOW);
        initMetaStreamEvent(metaStreamEvent, window.getWindowDefinition());
        MatchingMetaInfoHolder metaStreamInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                window.getWindowDefinition());
        CompiledCondition compiledCondition = window.compileCondition(onCondition,
                generateMatchingMetaInfoHolder(metaStreamEvent, window.getWindowDefinition()),
                variableExpressionExecutors, tableMap, siddhiQueryContext);
        FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(window, compiledCondition,
                siddhiQueryContext.getName(), metaStreamEvent);
        populateFindStoreQueryRuntime(findStoreQueryRuntime, metaStreamInfoHolder, storeQuery.getSelector(),
                variableExpressionExecutors, tableMap, windowMap, metaPosition,
                !storeQuery.getSelector().getGroupByList().isEmpty(), lockWrapper, siddhiQueryContext);
        return findStoreQueryRuntime;
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(AggregationRuntime aggregation, StoreQuery storeQuery,
                                                                Map<String, Table> tableMap,
                                                                Map<String, Window> windowMap,
                                                                Within within, Expression per,
                                                                Expression onCondition,
                                                                MetaStreamEvent metaStreamEvent,
                                                                List<VariableExpressionExecutor>
                                                                        variableExpressionExecutors,
                                                                LockWrapper lockWrapper,
                                                                SiddhiQueryContext siddhiQueryContext) {
        int metaPosition;
        metaStreamEvent.setEventType(EventType.AGGREGATE);
        initMetaStreamEvent(metaStreamEvent, aggregation.getAggregationDefinition());
        MatchingMetaInfoHolder metaStreamInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                aggregation.getAggregationDefinition());
        CompiledCondition compiledCondition = aggregation.compileExpression(onCondition, within, per,
                metaStreamInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
        metaStreamInfoHolder = ((IncrementalAggregateCompileCondition) compiledCondition).
                getAlteredMatchingMetaInfoHolder();
        FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(aggregation, compiledCondition,
                siddhiQueryContext.getName(), metaStreamEvent, siddhiQueryContext);
        metaPosition = 1;
        populateFindStoreQueryRuntime(findStoreQueryRuntime, metaStreamInfoHolder,
                storeQuery.getSelector(), variableExpressionExecutors, tableMap, windowMap,
                metaPosition, !storeQuery.getSelector().getGroupByList().isEmpty(), lockWrapper, siddhiQueryContext);
        ComplexEventPopulater complexEventPopulater = StreamEventPopulaterFactory.constructEventPopulator(
                metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvent(0), 0,
                ((IncrementalAggregateCompileCondition) compiledCondition).getAdditionalAttributes());
        ((IncrementalAggregateCompileCondition) compiledCondition)
                .setComplexEventPopulater(complexEventPopulater);
        return findStoreQueryRuntime;
    }

    private static StoreQueryRuntime constructStoreQueryRuntime(Table table, StoreQuery storeQuery,
                                                                Map<String, Table> tableMap,
                                                                Map<String, Window> windowMap,
                                                                int metaPosition,
                                                                Expression onCondition,
                                                                MetaStreamEvent metaStreamEvent,
                                                                List<VariableExpressionExecutor>
                                                                        variableExpressionExecutors,
                                                                LockWrapper lockWrapper,
                                                                SiddhiQueryContext siddhiQueryContext) {
        if (table instanceof QueryableProcessor && storeQuery.getType() == StoreQuery.StoreQueryType.FIND) {
            try {
                return constructOptimizedStoreQueryRuntime(table, storeQuery, tableMap,
                        metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors, siddhiQueryContext);
            //In case of error, we try to create the regular store query runtime.
            } catch (SiddhiAppCreationException | QueryableRecordTableException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Store Query optimization failed for table: "
                            + table.getTableDefinition().getId() + ". Creating Store Query runtime in normal mode. " +
                            "Reason for failure: " + e.getMessage());
                }
                return constructRegularStoreQueryRuntime(table, storeQuery, tableMap, windowMap,
                        metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                        lockWrapper, siddhiQueryContext);
            }
        } else {
            return constructRegularStoreQueryRuntime(table, storeQuery, tableMap, windowMap,
                    metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                    lockWrapper, siddhiQueryContext);
        }
    }

    private static StoreQueryRuntime constructOptimizedStoreQueryRuntime(Table table,
                                                                         StoreQuery storeQuery,
                                                                         Map<String, Table> tableMap,
                                                                         int metaPosition, Expression onCondition,
                                                                         MetaStreamEvent metaStreamEvent,
                                                                         List<VariableExpressionExecutor>
                                                                                 variableExpressionExecutors,
                                                                         SiddhiQueryContext siddhiQueryContext) {
        MatchingMetaInfoHolder matchingMetaInfoHolder;

        initMetaStreamEvent(metaStreamEvent, table.getTableDefinition());
        matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, table.getTableDefinition());
        CompiledCondition compiledCondition = table.compileCondition(onCondition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
        List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(storeQuery,
                tableMap, metaPosition, matchingMetaInfoHolder, siddhiQueryContext);

        MatchingMetaInfoHolder matchingMetaInfoHolderForSelection = generateMatchingMetaInfoHolder(
                metaStreamEvent, generateTableDefinitionFromStoreQuery(storeQuery, expectedOutputAttributes),
                table.getTableDefinition());
        CompiledSelection compiledSelection = ((QueryableProcessor) table).compileSelection(
                storeQuery.getSelector(), expectedOutputAttributes, matchingMetaInfoHolderForSelection,
                variableExpressionExecutors, tableMap, siddhiQueryContext);

        StoreQueryRuntime storeQueryRuntime =
                new SelectStoreQueryRuntime((QueryableProcessor) table, compiledCondition,
                        compiledSelection, expectedOutputAttributes, siddhiQueryContext.getName());
        QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                variableExpressionExecutors);
        return storeQueryRuntime;
    }

    private static StoreQueryRuntime constructRegularStoreQueryRuntime(Table table, StoreQuery storeQuery,
                                                                       Map<String, Table> tableMap,
                                                                       Map<String, Window> windowMap,
                                                                       int metaPosition, Expression onCondition,
                                                                       MetaStreamEvent metaStreamEvent,
                                                                       List<VariableExpressionExecutor>
                                                                               variableExpressionExecutors,
                                                                       LockWrapper lockWrapper,
                                                                       SiddhiQueryContext siddhiQueryContext) {
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
                        variableExpressionExecutors, tableMap, siddhiQueryContext);

                FindStoreQueryRuntime findStoreQueryRuntime = new FindStoreQueryRuntime(table, compiledCondition,
                        siddhiQueryContext.getName(), metaStreamEvent);
                populateFindStoreQueryRuntime(findStoreQueryRuntime, matchingMetaInfoHolder,
                        storeQuery.getSelector(), variableExpressionExecutors,
                        tableMap, windowMap, metaPosition,  !storeQuery.getSelector().getGroupByList().isEmpty(),
                        lockWrapper, siddhiQueryContext);
                return findStoreQueryRuntime;
            case INSERT:
                initMetaStreamEvent(metaStreamEvent, getInputDefinition(storeQuery, table));
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                        table.getTableDefinition());
                querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, storeQuery, lockWrapper, siddhiQueryContext);

                InsertStoreQueryRuntime insertStoreQueryRuntime =
                        new InsertStoreQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                insertStoreQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
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
                        tableMap, windowMap, metaPosition, storeQuery, lockWrapper, siddhiQueryContext);

                DeleteStoreQueryRuntime deleteStoreQueryRuntime =
                        new DeleteStoreQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                deleteStoreQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
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
                        tableMap, windowMap, metaPosition, storeQuery, lockWrapper, siddhiQueryContext);

                UpdateStoreQueryRuntime updateStoreQueryRuntime =
                        new UpdateStoreQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                updateStoreQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
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
                        tableMap, windowMap, metaPosition, storeQuery, lockWrapper, siddhiQueryContext);

                UpdateOrInsertStoreQueryRuntime updateOrInsertIntoStoreQueryRuntime =
                        new UpdateOrInsertStoreQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                updateOrInsertIntoStoreQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
                updateOrInsertIntoStoreQueryRuntime.setSelector(querySelector);
                updateOrInsertIntoStoreQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                        .getOutputStreamDefinition().getAttributeList());
                return updateOrInsertIntoStoreQueryRuntime;
            default:
                return null;
        }
    }

    private static List<Attribute> buildExpectedOutputAttributes(
            StoreQuery storeQuery, Map<String, Table> tableMap,
            int metaPosition, MatchingMetaInfoHolder metaStreamInfoHolder, SiddhiQueryContext siddhiQueryContext) {
        MetaStateEvent selectMetaStateEvent =
                new MetaStateEvent(metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvents());
        SelectorParser.parse(storeQuery.getSelector(),
                new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS),
                selectMetaStateEvent, tableMap, new ArrayList<>(), metaPosition, ProcessingMode.BATCH,
                false, siddhiQueryContext);
        return selectMetaStateEvent.getOutputStreamDefinition().getAttributeList();
    }

    private static void populateFindStoreQueryRuntime(FindStoreQueryRuntime findStoreQueryRuntime,
                                                      MatchingMetaInfoHolder metaStreamInfoHolder, Selector selector,
                                                      List<VariableExpressionExecutor> variableExpressionExecutors,
                                                      Map<String, Table> tableMap, Map<String, Window> windowMap,
                                                      int metaPosition, boolean groupBy, LockWrapper lockWrapper,
                                                      SiddhiQueryContext siddhiQueryContext) {
        ReturnStream returnStream = new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS);
        QuerySelector querySelector = SelectorParser.parse(selector, returnStream,
                metaStreamInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutors,
                metaPosition, ProcessingMode.BATCH, false, siddhiQueryContext);
        PassThroughOutputRateLimiter rateLimiter = new PassThroughOutputRateLimiter(siddhiQueryContext.getName());
        rateLimiter.init(lockWrapper, groupBy, siddhiQueryContext);
        OutputCallback outputCallback = OutputParser.constructOutputCallback(returnStream,
                metaStreamInfoHolder.getMetaStateEvent().getOutputStreamDefinition(), tableMap, windowMap,
                true, siddhiQueryContext);
        rateLimiter.setOutputCallback(outputCallback);
        querySelector.setNextProcessor(rateLimiter);

        QueryParserHelper.reduceMetaComplexEvent(metaStreamInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(metaStreamInfoHolder.getMetaStateEvent(), variableExpressionExecutors);
        querySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(metaStreamInfoHolder.getMetaStateEvent()));
        findStoreQueryRuntime.setStateEventFactory(new StateEventFactory(metaStreamInfoHolder.getMetaStateEvent()));
        findStoreQueryRuntime.setSelector(querySelector);
        findStoreQueryRuntime.setOutputAttributes(metaStreamInfoHolder.getMetaStateEvent().
                getOutputStreamDefinition().getAttributeList());
    }

    private static QuerySelector getQuerySelector(MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                                  Map<String, Table> tableMap,
                                                  Map<String, Window> windowMap, int metaPosition,
                                                  StoreQuery storeQuery, LockWrapper lockWrapper,
                                                  SiddhiQueryContext siddhiQueryContext) {
        QuerySelector querySelector = SelectorParser.parse(storeQuery.getSelector(), storeQuery.getOutputStream(),
                matchingMetaInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutors, metaPosition,
                ProcessingMode.BATCH, false, siddhiQueryContext);

        PassThroughOutputRateLimiter rateLimiter = new PassThroughOutputRateLimiter(siddhiQueryContext.getName());
        rateLimiter.init(lockWrapper,  !storeQuery.getSelector().getGroupByList().isEmpty(), siddhiQueryContext);
        OutputCallback outputCallback = OutputParser.constructOutputCallback(storeQuery.getOutputStream(),
                matchingMetaInfoHolder.getMetaStateEvent().getOutputStreamDefinition(), tableMap, windowMap,
                true, siddhiQueryContext);
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
        for (Attribute attribute : expectedOutputAttributes) {
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


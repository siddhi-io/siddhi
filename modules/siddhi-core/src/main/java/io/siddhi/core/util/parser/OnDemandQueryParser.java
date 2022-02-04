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
import io.siddhi.core.config.SiddhiOnDemandQueryContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.MetaStreamEvent.EventType;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import io.siddhi.core.exception.OnDemandQueryCreationException;
import io.siddhi.core.exception.QueryableRecordTableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.DeleteOnDemandQueryRuntime;
import io.siddhi.core.query.FindOnDemandQueryRuntime;
import io.siddhi.core.query.InsertOnDemandQueryRuntime;
import io.siddhi.core.query.OnDemandQueryRuntime;
import io.siddhi.core.query.SelectOnDemandQueryRuntime;
import io.siddhi.core.query.UpdateOnDemandQueryRuntime;
import io.siddhi.core.query.UpdateOrInsertOnDemandQueryRuntime;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.ratelimit.PassThroughOutputRateLimiter;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.Table;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
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
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.query.OnDemandQuery;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class to parse {@link OnDemandQueryRuntime}.
 */
public class OnDemandQueryParser {

    /**
     * Parse a onDemandQuery and return corresponding OnDemandQueryRuntime.
     *
     * @param onDemandQuery       onDemandQuery to be parsed.
     * @param siddhiQueryContext associated Siddhi query context.
     * @param tableMap         keyvalue containing tables.
     * @param windowMap        keyvalue containing windows.
     * @param aggregationMap   keyvalue containing aggregation runtimes.
     * @return OnDemandQueryRuntime
     */
    private static final Logger log = LogManager.getLogger(OnDemandQueryParser.class);

    public static OnDemandQueryRuntime parse(OnDemandQuery onDemandQuery, String onDemandQueryString,
                                             SiddhiAppContext siddhiAppContext,
                                             Map<String, Table> tableMap, Map<String, Window> windowMap,
                                             Map<String, AggregationRuntime> aggregationMap) {

        final LockWrapper lockWrapper = new LockWrapper("OnDemandQueryLock");
        lockWrapper.setLock(new ReentrantLock());

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();

        int metaPosition = SiddhiConstants.UNKNOWN_STATE;
        String queryName;
        Table table;
        SiddhiQueryContext siddhiQueryContext;
        Expression onCondition;

        SnapshotService.getSkipStateStorageThreadLocal().set(true);

        switch (onDemandQuery.getType()) {
            case FIND:
                Within within = null;
                Expression per = null;
                queryName = "store_select_query_" + onDemandQuery.getInputStore().getStoreId();
                siddhiQueryContext = new SiddhiOnDemandQueryContext(siddhiAppContext, queryName, onDemandQueryString);
                InputStore inputStore = onDemandQuery.getInputStore();
                try {
                    onCondition = Expression.value(true);
                    metaStreamEvent.setInputReferenceId(inputStore.getStoreReferenceId());

                    if (inputStore instanceof AggregationInputStore) {
                        AggregationInputStore aggregationInputStore = (AggregationInputStore) inputStore;
                        if (aggregationMap.get(inputStore.getStoreId()) == null) {
                            throw new OnDemandQueryCreationException("Aggregation \"" + inputStore.getStoreId() +
                                    "\" has not been defined");
                        }
                        if (aggregationInputStore.getPer() != null && aggregationInputStore.getWithin() != null) {
                            within = aggregationInputStore.getWithin();
                            per = aggregationInputStore.getPer();
                        } else if (aggregationInputStore.getPer() != null ||
                                aggregationInputStore.getWithin() != null) {
                            throw new OnDemandQueryCreationException(
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
                        return constructOnDemandQueryRuntime(table, onDemandQuery, tableMap, windowMap,
                                metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors, lockWrapper,
                                siddhiQueryContext);
                    } else {
                        AggregationRuntime aggregation = aggregationMap.get(inputStore.getStoreId());
                        if (aggregation != null) {
                            return constructOnDemandQueryRuntime(aggregation, onDemandQuery, tableMap,
                                    windowMap, within, per, onCondition, metaStreamEvent,
                                    variableExpressionExecutors, lockWrapper, siddhiQueryContext);
                        } else {
                            Window window = windowMap.get(inputStore.getStoreId());
                            if (window != null) {
                                return constructOnDemandQueryRuntime(window, onDemandQuery,
                                        tableMap, windowMap, metaPosition, onCondition, metaStreamEvent,
                                        variableExpressionExecutors, lockWrapper, siddhiQueryContext);
                            } else {
                                throw new OnDemandQueryCreationException(
                                        inputStore.getStoreId() + " is neither a table, aggregation or window");
                            }
                        }
                    }

                } finally {
                    SnapshotService.getSkipStateStorageThreadLocal().set(null);
                }
            case INSERT:
                InsertIntoStream inserIntoStreamt = (InsertIntoStream) onDemandQuery.getOutputStream();
                queryName = "store_insert_query_" + inserIntoStreamt.getId();
                siddhiQueryContext = new SiddhiOnDemandQueryContext(siddhiAppContext, queryName, onDemandQueryString);
                onCondition = Expression.value(true);

                return getOnDemandQueryRuntime(onDemandQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, inserIntoStreamt, onCondition, siddhiQueryContext);
            case DELETE:
                DeleteStream deleteStream = (DeleteStream) onDemandQuery.getOutputStream();
                queryName = "store_delete_query_" + deleteStream.getId();
                siddhiQueryContext = new SiddhiOnDemandQueryContext(siddhiAppContext, queryName, onDemandQueryString);
                onCondition = deleteStream.getOnDeleteExpression();

                return getOnDemandQueryRuntime(onDemandQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, deleteStream, onCondition, siddhiQueryContext);
            case UPDATE:
                UpdateStream outputStream = (UpdateStream) onDemandQuery.getOutputStream();
                queryName = "store_update_query_" + outputStream.getId();
                siddhiQueryContext = new SiddhiOnDemandQueryContext(siddhiAppContext, queryName, onDemandQueryString);
                onCondition = outputStream.getOnUpdateExpression();

                return getOnDemandQueryRuntime(onDemandQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, outputStream, onCondition, siddhiQueryContext);
            case UPDATE_OR_INSERT:
                UpdateOrInsertStream onDemandQueryOutputStream = (UpdateOrInsertStream) onDemandQuery.getOutputStream();
                queryName = "store_update_or_insert_query_" + onDemandQueryOutputStream.getId();
                siddhiQueryContext = new SiddhiOnDemandQueryContext(siddhiAppContext, queryName, onDemandQueryString);
                onCondition = onDemandQueryOutputStream.getOnUpdateExpression();

                return getOnDemandQueryRuntime(onDemandQuery, tableMap, windowMap, metaPosition,
                        lockWrapper, metaStreamEvent, onDemandQueryOutputStream, onCondition, siddhiQueryContext);
            default:
                return null;
        }
    }

    private static OnDemandQueryRuntime getOnDemandQueryRuntime(OnDemandQuery onDemandQuery,
                                                                Map<String, Table> tableMap,
                                                                Map<String, Window> windowMap,
                                                                int metaPosition, LockWrapper lockWrapper,
                                                                MetaStreamEvent metaStreamEvent,
                                                                OutputStream outputStream, Expression onCondition,
                                                                SiddhiQueryContext siddhiQueryContext) {
        try {
            List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
            Table table = tableMap.get(outputStream.getId());

            if (table != null) {
                return constructOnDemandQueryRuntime(table, onDemandQuery, tableMap, windowMap,
                        metaPosition, onCondition, metaStreamEvent,
                        variableExpressionExecutors, lockWrapper, siddhiQueryContext);
            } else {
                throw new OnDemandQueryCreationException(outputStream.getId() + " is not a table.");
            }

        } finally {
            SnapshotService.getSkipStateStorageThreadLocal().set(null);
        }
    }

    private static OnDemandQueryRuntime constructOnDemandQueryRuntime(
            Window window, OnDemandQuery onDemandQuery,
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
        FindOnDemandQueryRuntime findOnDemandQueryRuntime = new FindOnDemandQueryRuntime(window, compiledCondition,
                siddhiQueryContext.getName(), metaStreamEvent);
        populateFindOnDemandQueryRuntime(findOnDemandQueryRuntime, metaStreamInfoHolder, onDemandQuery.getSelector(),
                variableExpressionExecutors, tableMap, windowMap, metaPosition,
                !onDemandQuery.getSelector().getGroupByList().isEmpty(), lockWrapper, siddhiQueryContext);
        return findOnDemandQueryRuntime;
    }

    private static OnDemandQueryRuntime constructOnDemandQueryRuntime(AggregationRuntime aggregation,
                                                                      OnDemandQuery onDemandQuery,
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
                onDemandQuery.getSelector().getGroupByList(), metaStreamInfoHolder, variableExpressionExecutors,
                tableMap, siddhiQueryContext);
        ((IncrementalAggregateCompileCondition) compiledCondition).init();
        metaStreamInfoHolder = ((IncrementalAggregateCompileCondition) compiledCondition).
                getAlteredMatchingMetaInfoHolder();
        FindOnDemandQueryRuntime findOnDemandQueryRuntime = new FindOnDemandQueryRuntime(aggregation, compiledCondition,
                siddhiQueryContext.getName(), metaStreamEvent, siddhiQueryContext);
        metaPosition = 1;
        populateFindOnDemandQueryRuntime(findOnDemandQueryRuntime, metaStreamInfoHolder,
                onDemandQuery.getSelector(), variableExpressionExecutors, tableMap, windowMap,
                metaPosition, !onDemandQuery.getSelector().getGroupByList().isEmpty(), lockWrapper, siddhiQueryContext);
        ComplexEventPopulater complexEventPopulater = StreamEventPopulaterFactory.constructEventPopulator(
                metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvent(0), 0,
                ((IncrementalAggregateCompileCondition) compiledCondition).getAdditionalAttributes());
        ((IncrementalAggregateCompileCondition) compiledCondition)
                .setComplexEventPopulater(complexEventPopulater);
        return findOnDemandQueryRuntime;
    }

    private static OnDemandQueryRuntime constructOnDemandQueryRuntime(Table table, OnDemandQuery onDemandQuery,
                                                                      Map<String, Table> tableMap,
                                                                      Map<String, Window> windowMap,
                                                                      int metaPosition,
                                                                      Expression onCondition,
                                                                      MetaStreamEvent metaStreamEvent,
                                                                      List<VariableExpressionExecutor>
                                                                              variableExpressionExecutors,
                                                                      LockWrapper lockWrapper,
                                                                      SiddhiQueryContext siddhiQueryContext) {
        metaStreamEvent.setEventType(EventType.TABLE);
        if (table instanceof QueryableProcessor && onDemandQuery.getType() == OnDemandQuery.OnDemandQueryType.FIND) {
            try {
                return constructOptimizedOnDemandQueryRuntime(table, onDemandQuery, tableMap,
                        metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors, siddhiQueryContext);
                //In case of error, we try to create the regular on-demand query runtime.
            } catch (SiddhiAppCreationException | SiddhiAppValidationException | QueryableRecordTableException e) {
                if (log.isDebugEnabled()) {
                    log.debug("On-demand query optimization failed for table: "
                            + table.getTableDefinition().getId() + ". Creating On-demand query runtime in normal mode. " +
                            "Reason for failure: " + e.getMessage());
                }
                return constructRegularOnDemandQueryRuntime(table, onDemandQuery, tableMap, windowMap,
                        metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                        lockWrapper, siddhiQueryContext);
            }
        } else {
            return constructRegularOnDemandQueryRuntime(table, onDemandQuery, tableMap, windowMap,
                    metaPosition, onCondition, metaStreamEvent, variableExpressionExecutors,
                    lockWrapper, siddhiQueryContext);
        }
    }

    private static OnDemandQueryRuntime constructOptimizedOnDemandQueryRuntime(Table table,
                                                                               OnDemandQuery onDemandQuery,
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
        List<Attribute> expectedOutputAttributes = buildExpectedOutputAttributes(onDemandQuery,
                tableMap, metaPosition, matchingMetaInfoHolder, siddhiQueryContext);

//        MatchingMetaInfoHolder matchingMetaInfoHolderForSelection = generateMatchingMetaInfoHolder(
//                metaStreamEvent, generateTableDefinitionFromOnDemandQuery(onDemandQuery, expectedOutputAttributes),
//                table.getTableDefinition());
        CompiledSelection compiledSelection = ((QueryableProcessor) table).compileSelection(
                onDemandQuery.getSelector(), expectedOutputAttributes, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
        OnDemandQueryRuntime onDemandQueryRuntime =
                new SelectOnDemandQueryRuntime((QueryableProcessor) table, compiledCondition,
                        compiledSelection, expectedOutputAttributes, siddhiQueryContext.getName());
        try {
            AbstractQueryableRecordTable.CompiledSelectionWithCache compiledSelectionWithCache =
                    (AbstractQueryableRecordTable.CompiledSelectionWithCache) compiledSelection;
            onDemandQueryRuntime.setSelector(compiledSelectionWithCache.getQuerySelector());
            onDemandQueryRuntime.setMetaStreamEvent(metaStreamEvent);
            onDemandQueryRuntime.setStateEventFactory(new StateEventFactory(
                    matchingMetaInfoHolder.getMetaStateEvent()));
        } catch (ClassCastException ignored) {

        }

        QueryParserHelper.reduceMetaComplexEvent(matchingMetaInfoHolder.getMetaStateEvent());
        QueryParserHelper.updateVariablePosition(matchingMetaInfoHolder.getMetaStateEvent(),
                variableExpressionExecutors);
        return onDemandQueryRuntime;
    }

    private static OnDemandQueryRuntime constructRegularOnDemandQueryRuntime(Table table, OnDemandQuery onDemandQuery,
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

        switch (onDemandQuery.getType()) {
            case FIND:
                initMetaStreamEvent(metaStreamEvent, table.getTableDefinition());
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                        table.getTableDefinition());
                CompiledCondition compiledCondition = table.compileCondition(onCondition, matchingMetaInfoHolder,
                        variableExpressionExecutors, tableMap, siddhiQueryContext);

                FindOnDemandQueryRuntime findOnDemandQueryRuntime = new FindOnDemandQueryRuntime(table, compiledCondition,
                        siddhiQueryContext.getName(), metaStreamEvent);
                populateFindOnDemandQueryRuntime(findOnDemandQueryRuntime, matchingMetaInfoHolder,
                        onDemandQuery.getSelector(), variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, !onDemandQuery.getSelector().getGroupByList().isEmpty(),
                        lockWrapper, siddhiQueryContext);
                return findOnDemandQueryRuntime;
            case INSERT:
                initMetaStreamEvent(metaStreamEvent, getInputDefinition(onDemandQuery, table));
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                        table.getTableDefinition());
                querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, onDemandQuery, lockWrapper, siddhiQueryContext);

                InsertOnDemandQueryRuntime insertOnDemandQueryRuntime =
                        new InsertOnDemandQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                insertOnDemandQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
                insertOnDemandQueryRuntime.setSelector(querySelector);
                insertOnDemandQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                        .getOutputStreamDefinition().getAttributeList());
                return insertOnDemandQueryRuntime;
            case DELETE:
                inputDefinition = getInputDefinition(onDemandQuery, table);
                initMetaStreamEvent(metaStreamEvent, inputDefinition);
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent,
                        inputDefinition, table.getTableDefinition());
                querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, onDemandQuery, lockWrapper, siddhiQueryContext);

                DeleteOnDemandQueryRuntime deleteOnDemandQueryRuntime =
                        new DeleteOnDemandQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                deleteOnDemandQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
                deleteOnDemandQueryRuntime.setSelector(querySelector);
                deleteOnDemandQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                        .getOutputStreamDefinition().getAttributeList());
                return deleteOnDemandQueryRuntime;
            case UPDATE:
                inputDefinition = getInputDefinition(onDemandQuery, table);
                initMetaStreamEvent(metaStreamEvent, inputDefinition);
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, inputDefinition,
                        table.getTableDefinition());
                querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, onDemandQuery, lockWrapper, siddhiQueryContext);

                UpdateOnDemandQueryRuntime updateOnDemandQueryRuntime =
                        new UpdateOnDemandQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                updateOnDemandQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
                updateOnDemandQueryRuntime.setSelector(querySelector);
                updateOnDemandQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                        .getOutputStreamDefinition().getAttributeList());
                return updateOnDemandQueryRuntime;
            case UPDATE_OR_INSERT:
                inputDefinition = getInputDefinition(onDemandQuery, table);
                initMetaStreamEvent(metaStreamEvent, inputDefinition);
                matchingMetaInfoHolder = generateMatchingMetaInfoHolder(metaStreamEvent, inputDefinition,
                        table.getTableDefinition());
                querySelector = getQuerySelector(matchingMetaInfoHolder, variableExpressionExecutors,
                        tableMap, windowMap, metaPosition, onDemandQuery, lockWrapper, siddhiQueryContext);

                UpdateOrInsertOnDemandQueryRuntime updateOrInsertIntoOnDemandQueryRuntime =
                        new UpdateOrInsertOnDemandQueryRuntime(siddhiQueryContext.getName(), metaStreamEvent);
                updateOrInsertIntoOnDemandQueryRuntime.setStateEventFactory(
                        new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent()));
                updateOrInsertIntoOnDemandQueryRuntime.setSelector(querySelector);
                updateOrInsertIntoOnDemandQueryRuntime.setOutputAttributes(matchingMetaInfoHolder.getMetaStateEvent()
                        .getOutputStreamDefinition().getAttributeList());
                return updateOrInsertIntoOnDemandQueryRuntime;
            default:
                return null;
        }
    }

    public static List<Attribute> buildExpectedOutputAttributes(
            OnDemandQuery onDemandQuery, Map<String, Table> tableMap,
            int metaPosition, MatchingMetaInfoHolder metaStreamInfoHolder, SiddhiQueryContext siddhiQueryContext) {
        MetaStateEvent selectMetaStateEvent =
                new MetaStateEvent(metaStreamInfoHolder.getMetaStateEvent().getMetaStreamEvents());
        SelectorParser.parse(onDemandQuery.getSelector(),
                new ReturnStream(OutputStream.OutputEventType.CURRENT_EVENTS),
                selectMetaStateEvent, tableMap, new ArrayList<>(), metaPosition, ProcessingMode.BATCH,
                false, siddhiQueryContext);
        return selectMetaStateEvent.getOutputStreamDefinition().getAttributeList();
    }

    private static void populateFindOnDemandQueryRuntime(FindOnDemandQueryRuntime findOnDemandQueryRuntime,
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
        findOnDemandQueryRuntime.setStateEventFactory(new StateEventFactory(metaStreamInfoHolder.getMetaStateEvent()));
        findOnDemandQueryRuntime.setSelector(querySelector);
        findOnDemandQueryRuntime.setOutputAttributes(metaStreamInfoHolder.getMetaStateEvent().
                getOutputStreamDefinition().getAttributeList());
    }

    private static QuerySelector getQuerySelector(MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                                  Map<String, Table> tableMap,
                                                  Map<String, Window> windowMap, int metaPosition,
                                                  OnDemandQuery onDemandQuery, LockWrapper lockWrapper,
                                                  SiddhiQueryContext siddhiQueryContext) {
        QuerySelector querySelector = SelectorParser.parse(onDemandQuery.getSelector(), onDemandQuery.getOutputStream(),
                matchingMetaInfoHolder.getMetaStateEvent(), tableMap, variableExpressionExecutors, metaPosition,
                ProcessingMode.BATCH, false, siddhiQueryContext);

        PassThroughOutputRateLimiter rateLimiter = new PassThroughOutputRateLimiter(siddhiQueryContext.getName());
        rateLimiter.init(lockWrapper, !onDemandQuery.getSelector().getGroupByList().isEmpty(), siddhiQueryContext);
        OutputCallback outputCallback = OutputParser.constructOutputCallback(onDemandQuery.getOutputStream(),
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

    private static AbstractDefinition generateTableDefinitionFromOnDemandQuery(OnDemandQuery onDemandQuery,
                                                                               List<Attribute> expectedOutputAttributes) {
        TableDefinition tableDefinition = TableDefinition.id(onDemandQuery.getInputStore().getStoreId());
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

    public static MatchingMetaInfoHolder generateMatchingMetaInfoHolderForCacheTable(TableDefinition tableDefinition) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(1);
        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        initMetaStreamEvent(metaStreamEvent, tableDefinition);
        metaStateEvent.addEvent(metaStreamEvent);
        MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(metaStateEvent,
                -1, 0, tableDefinition, tableDefinition, 0);
        return matchingMetaInfoHolder;
    }

    private static void initMetaStreamEvent(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition) {
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.initializeOnAfterWindowData();
        inputDefinition.getAttributeList().forEach(metaStreamEvent::addData);
    }

    private static AbstractDefinition getInputDefinition(OnDemandQuery onDemandQuery, Table table) {
        if (onDemandQuery.getSelector().getSelectionList().isEmpty()) {
            return table.getTableDefinition();
        } else {
            StreamDefinition streamDefinition = new StreamDefinition();
            streamDefinition.setId(table.getTableDefinition().getId() + "InputStream");
            return streamDefinition;
        }
    }
}


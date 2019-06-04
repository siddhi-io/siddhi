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

package org.wso2.siddhi.core.aggregation;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.selector.GroupByKeyGenerator;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.IncrementalAggregateCompileCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.parser.ExpressionParser;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;
import org.wso2.siddhi.core.util.statistics.MemoryCalculable;
import org.wso2.siddhi.core.util.statistics.ThroughputTracker;
import org.wso2.siddhi.core.util.statistics.metrics.Level;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.aggregation.Within;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.AggregationDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.AttributeFunction;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.wso2.siddhi.core.util.ExpressionExecutorClonerUtil.getExpressionExecutorClones;
import static org.wso2.siddhi.core.util.SiddhiConstants.UNKNOWN_STATE;
import static org.wso2.siddhi.query.api.expression.Expression.Time.normalizeDuration;

/**
 * Aggregation runtime managing aggregation operations for aggregation definition.
 */
public class AggregationRuntime implements MemoryCalculable {
    private AggregationDefinition aggregationDefinition;
    private boolean isProcessingOnExternalTime;
    private boolean isDistributed;
    private List<TimePeriod.Duration> incrementalDurations;
    private Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap;
    private Map<TimePeriod.Duration, Table> aggregationTables;
    private List<String> tableAttributesNameList;
    private MetaStreamEvent aggregateMetaSteamEvent;
    private List<ExpressionExecutor> outputExpressionExecutors;
    private Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap;
    private ExpressionExecutor shouldUpdateTimestampExpressionExecutor;
    private Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList;

    private IncrementalDataPurger incrementalDataPurger;
    private IncrementalExecutorsInitialiser incrementalExecutorsInitialiser;

    private SingleStreamRuntime singleStreamRuntime;
    private SiddhiAppContext siddhiAppContext;

    private LatencyTracker latencyTrackerFind;
    private ThroughputTracker throughputTrackerFind;

    private boolean isFirstEventArrived;

    public AggregationRuntime(AggregationDefinition aggregationDefinition, boolean isProcessingOnExternalTime,
                              boolean isDistributed, List<TimePeriod.Duration> incrementalDurations,
                              Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap,
                              Map<TimePeriod.Duration, Table> aggregationTables,
                              List<ExpressionExecutor> outputExpressionExecutors,
                              Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap,
                              ExpressionExecutor shouldUpdateTimestampExpressionExecutor,
                              Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList,
                              IncrementalDataPurger incrementalDataPurger,
                              IncrementalExecutorsInitialiser incrementalExecutorInitialiser,
                              SingleStreamRuntime singleStreamRuntime, SiddhiAppContext siddhiAppContext,
                              MetaStreamEvent tableMetaStreamEvent, LatencyTracker latencyTrackerFind,
                              ThroughputTracker throughputTrackerFind) {

        this.aggregationDefinition = aggregationDefinition;
        this.isProcessingOnExternalTime = isProcessingOnExternalTime;
        this.isDistributed = isDistributed;
        this.incrementalDurations = incrementalDurations;
        this.incrementalExecutorMap = incrementalExecutorMap;
        this.aggregationTables = aggregationTables;
        this.tableAttributesNameList = tableMetaStreamEvent.getInputDefinitions().get(0).getAttributeList()
                .stream().map(Attribute::getName).collect(Collectors.toList());
        this.outputExpressionExecutors = outputExpressionExecutors;
        this.aggregateProcessingExecutorsMap = aggregateProcessingExecutorsMap;
        this.shouldUpdateTimestampExpressionExecutor = shouldUpdateTimestampExpressionExecutor;
        this.groupByKeyGeneratorList = groupByKeyGeneratorList;

        this.incrementalDataPurger = incrementalDataPurger;
        this.incrementalExecutorsInitialiser = incrementalExecutorInitialiser;

        this.singleStreamRuntime = singleStreamRuntime;
        this.siddhiAppContext = siddhiAppContext;
        this.aggregateMetaSteamEvent = new MetaStreamEvent();
        aggregationDefinition.getAttributeList().forEach(this.aggregateMetaSteamEvent::addOutputData);

        this.latencyTrackerFind = latencyTrackerFind;
        this.throughputTrackerFind = throughputTrackerFind;
    }

    private static void initMetaStreamEvent(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                            String inputReferenceId) {

        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.setInputReferenceId(inputReferenceId);
        metaStreamEvent.initializeAfterWindowData();
        inputDefinition.getAttributeList().forEach(metaStreamEvent::addData);
    }

    private static MetaStreamEvent alterMetaStreamEvent(boolean isStoreQuery, MetaStreamEvent originalMetaStreamEvent,
                                                        List<Attribute> additionalAttributes) {

        StreamDefinition alteredStreamDef = new StreamDefinition();

        if (!isStoreQuery) {
            for (Attribute attribute : originalMetaStreamEvent.getLastInputDefinition().getAttributeList()) {
                alteredStreamDef.attribute(attribute.getName(), attribute.getType());
            }
        }
        additionalAttributes.forEach(attribute -> alteredStreamDef.attribute(attribute.getName(), attribute.getType()));

        initMetaStreamEvent(originalMetaStreamEvent, alteredStreamDef, originalMetaStreamEvent.getInputReferenceId());
        return originalMetaStreamEvent;
    }

    private static MetaStreamEvent createMetaStoreEvent(AbstractDefinition tableDefinition, String referenceId) {
        MetaStreamEvent metaStreamEventForTable = new MetaStreamEvent();
        metaStreamEventForTable.setEventType(MetaStreamEvent.EventType.TABLE);
        initMetaStreamEvent(metaStreamEventForTable, tableDefinition, referenceId);
        return metaStreamEventForTable;
    }

    private static MatchingMetaInfoHolder alterMetaInfoHolderForStoreQuery(
            MetaStreamEvent newMetaStreamEventWithStartEnd, MatchingMetaInfoHolder matchingMetaInfoHolder) {

        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        MetaStreamEvent incomingMetaStreamEvent = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0);
        metaStateEvent.addEvent(newMetaStreamEventWithStartEnd);
        metaStateEvent.addEvent(incomingMetaStreamEvent);

        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1,
                newMetaStreamEventWithStartEnd.getLastInputDefinition(),
                incomingMetaStreamEvent.getLastInputDefinition(), UNKNOWN_STATE);
    }

    private static MatchingMetaInfoHolder createNewStreamTableMetaInfoHolder(
            MetaStreamEvent metaStreamEvent, MetaStreamEvent metaStoreEvent) {

        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        metaStateEvent.addEvent(metaStreamEvent);
        metaStateEvent.addEvent(metaStoreEvent);
        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1,
                metaStreamEvent.getLastInputDefinition(), metaStoreEvent.getLastInputDefinition(), UNKNOWN_STATE);
    }

    public AggregationDefinition getAggregationDefinition() {
        return aggregationDefinition;
    }

    public SingleStreamRuntime getSingleStreamRuntime() {
        return singleStreamRuntime;
    }

    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {

        try {
            SnapshotService.getSkipSnapshotableThreadLocal().set(true);
            if (latencyTrackerFind != null && Level.DETAIL.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerFind.markIn();
                throughputTrackerFind.eventIn();
            }
            if (!isDistributed && !isFirstEventArrived) {
                initialiseIncrementalExecutors(false);
            }
            return ((IncrementalAggregateCompileCondition) compiledCondition).find(matchingEvent,
                    incrementalExecutorMap, aggregateProcessingExecutorsMap, groupByKeyGeneratorList,
                    shouldUpdateTimestampExpressionExecutor);
        } finally {
            SnapshotService.getSkipSnapshotableThreadLocal().set(null);
            if (latencyTrackerFind != null && Level.DETAIL.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerFind.markOut();
            }
        }
    }

    public CompiledCondition compileExpression(Expression expression, Within within, Expression per,
                                               MatchingMetaInfoHolder matchingMetaInfoHolder,
                                               List<VariableExpressionExecutor> variableExpressionExecutors,
                                               Map<String, Table> tableMap, String queryName,
                                               SiddhiAppContext siddhiAppContext) {

        Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions = new HashMap<>();
        CompiledCondition withinInMemoryCompileCondition;
        CompiledCondition onCompiledCondition;

        List<Attribute> additionalAttributes = new ArrayList<>();

        // Define additional attribute list
        additionalAttributes.add(new Attribute("_START", Attribute.Type.LONG));
        additionalAttributes.add(new Attribute("_END", Attribute.Type.LONG));

        int lowerGranularitySize = this.incrementalDurations.size() - 1;
        List<String> lowerGranularityAttributes = new ArrayList<>();
        if (isDistributed) {
            //Add additional attributes to get base aggregation timestamps based on current timestamps
            // for values calculated in in - memory in the shards
            for (int i = 0; i < lowerGranularitySize; i++) {
                String attributeName = "_AGG_TIMESTAMP_FILTER_" + i;
                additionalAttributes.add(new Attribute(attributeName, Attribute.Type.LONG));
                lowerGranularityAttributes.add(attributeName);
            }
        }

        // Get table definition. Table definitions for all the tables used to persist aggregates are similar.
        // Therefore it's enough to get the definition from one table.
        AbstractDefinition tableDefinition = aggregationTables.get(incrementalDurations.get(0)).getTableDefinition();

        boolean isStoreQuery = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvents().length == 1;

        // Alter existing meta stream event or create new one if a meta stream doesn't exist
        // After calling this method the original MatchingMetaInfoHolder's meta stream event would be altered
        // Alter meta info holder to contain stream event and aggregate both when it's a store query
        MetaStreamEvent metaStreamEventForTableLookups;
        if (isStoreQuery) {
            metaStreamEventForTableLookups = alterMetaStreamEvent(true, new MetaStreamEvent(), additionalAttributes);
            matchingMetaInfoHolder = alterMetaInfoHolderForStoreQuery(metaStreamEventForTableLookups,
                    matchingMetaInfoHolder);
        } else {
            metaStreamEventForTableLookups = alterMetaStreamEvent(false,
                    matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0), additionalAttributes);
        }

        MetaStreamEvent metaStoreEventForTableLookups = createMetaStoreEvent(tableDefinition,
                matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(1).getInputReferenceId());

        // Create new MatchingMetaInfoHolder containing metaStreamEventForTableLookups and table meta event
        MatchingMetaInfoHolder metaInfoHolderForTableLookups = createNewStreamTableMetaInfoHolder(
                metaStreamEventForTableLookups, metaStoreEventForTableLookups);

        // Create per expression executor
        ExpressionExecutor perExpressionExecutor;
        if (per != null) {
            perExpressionExecutor = ExpressionParser.parseExpression(per, matchingMetaInfoHolder.getMetaStateEvent(),
                    matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors, siddhiAppContext,
                    false, 0, queryName);
            if (perExpressionExecutor.getReturnType() != Attribute.Type.STRING) {
                throw new SiddhiAppCreationException(
                        "Query " + queryName + "'s per value expected a string but found "
                                + perExpressionExecutor.getReturnType(),
                        per.getQueryContextStartIndex(), per.getQueryContextEndIndex());
            }
            // Additional Per time function verification at compile time if it is a constant
            if (perExpressionExecutor instanceof ConstantExpressionExecutor) {
                String perValue = ((ConstantExpressionExecutor) perExpressionExecutor).getValue().toString();
                try {
                    normalizeDuration(perValue);
                } catch (SiddhiAppValidationException e) {
                    throw new SiddhiAppValidationException(
                            "Aggregation Query's per value is expected to be of a valid time function of the " +
                                    "following " + TimePeriod.Duration.SECONDS + ", " + TimePeriod.Duration.MINUTES
                                    + ", " + TimePeriod.Duration.HOURS + ", " + TimePeriod.Duration.DAYS + ", "
                                    + TimePeriod.Duration.MONTHS + ", " + TimePeriod.Duration.YEARS + ".");
                }
            }
        } else {
            throw new SiddhiAppCreationException("Syntax Error: Aggregation join query must contain a `per` " +
                    "definition for granularity");
        }

        // Create start and end time expression
        Expression startEndTimeExpression;
        ExpressionExecutor startTimeEndTimeExpressionExecutor;
        if (within != null) {
            if (within.getTimeRange().size() == 1) {
                startEndTimeExpression = new AttributeFunction("incrementalAggregator",
                        "startTimeEndTime", within.getTimeRange().get(0));
            } else { // within.getTimeRange().size() == 2
                startEndTimeExpression = new AttributeFunction("incrementalAggregator",
                        "startTimeEndTime", within.getTimeRange().get(0), within.getTimeRange().get(1));
            }
            startTimeEndTimeExpressionExecutor = ExpressionParser.parseExpression(startEndTimeExpression,
                    matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                    variableExpressionExecutors, siddhiAppContext, false, 0, queryName);
        } else {
            throw new SiddhiAppCreationException("Syntax Error : Aggregation read query must contain a `within` " +
                    "definition for filtering of aggregation data.");
        }

        // Create within expression
        Expression timeFilterExpression;
        if (isProcessingOnExternalTime) {
            timeFilterExpression = Expression.variable("AGG_EVENT_TIMESTAMP");
        } else {
            timeFilterExpression = Expression.variable("AGG_TIMESTAMP");
        }
        Expression withinExpression;
        Expression start = Expression.variable(additionalAttributes.get(0).getName());
        Expression end = Expression.variable(additionalAttributes.get(1).getName());
        Expression compareWithStartTime = Compare.compare(start, Compare.Operator.LESS_THAN_EQUAL,
                timeFilterExpression);
        Expression compareWithEndTime = Compare.compare(timeFilterExpression, Compare.Operator.LESS_THAN, end);
        withinExpression = Expression.and(compareWithStartTime, compareWithEndTime);


        List<ExpressionExecutor> timestampFilterExecutors = new ArrayList<>();
        if (isDistributed) {
            for (int i = 0; i < lowerGranularitySize; i++) {
                Expression[] expressionArray = new Expression[]{
                        new AttributeFunction("", "currentTimeMillis", null),
                        Expression.value(this.incrementalDurations.get(i + 1).toString())};
                Expression filterExpression = new AttributeFunction("incrementalAggregator",
                        "getAggregationStartTime", expressionArray);
                timestampFilterExecutors.add(ExpressionParser.parseExpression(filterExpression,
                        matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                        variableExpressionExecutors, siddhiAppContext, false, 0, queryName));
            }
        }

        // Create compile condition per each table used to persist aggregates.
        // These compile conditions are used to check whether the aggregates in tables are within the given duration.
        // Combine with and on condition for table query
        AggregationExpressionBuilder aggregationExpressionBuilder = new AggregationExpressionBuilder(expression);
        AggregationExpressionVisitor expressionVisitor = new AggregationExpressionVisitor(
                metaStreamEventForTableLookups.getInputReferenceId(),
                metaStreamEventForTableLookups.getLastInputDefinition().getAttributeList(),
                this.tableAttributesNameList
        );
        aggregationExpressionBuilder.build(expressionVisitor);
        Expression reducedExpression = expressionVisitor.getReducedExpression();
        Expression withinExpressionTable = Expression.and(withinExpression, reducedExpression);
        List<VariableExpressionExecutor> startEndExpressionExecutorList = new ArrayList<>();
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            CompiledCondition withinTableCompileCondition = entry.getValue().compileCondition(withinExpressionTable,
                    metaInfoHolderForTableLookups, siddhiAppContext, startEndExpressionExecutorList, tableMap,
                    queryName);
            withinTableCompiledConditions.put(entry.getKey(), withinTableCompileCondition);
        }

        // Create compile condition for in-memory data.
        // This compile condition is used to check whether the running aggregates (in-memory data)
        // are within given duration
        withinInMemoryCompileCondition = OperatorParser.constructOperator(new ComplexEventChunk<>(true),
                withinExpression, metaInfoHolderForTableLookups, siddhiAppContext,
                startEndExpressionExecutorList, tableMap, queryName);

        // Create compile condition for in-memory data, in case of distributed
        // Look at the lower level granularities
        Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition = new HashMap<>();
        String aggregationName = aggregationDefinition.getId();
        Expression lowerGranularity;
        if (isDistributed) {
            for (int i = 0; i < lowerGranularitySize; i++) {
                if (isProcessingOnExternalTime) {
                    lowerGranularity = Expression.and(
                            Expression.compare(
                                    Expression.variable("AGG_TIMESTAMP"),
                                    Compare.Operator.GREATER_THAN_EQUAL,
                                    Expression.variable(lowerGranularityAttributes.get(i))),
                            withinExpressionTable
                    );
                } else {
                    lowerGranularity = Expression.and(
                            Expression.compare(
                                    Expression.variable("AGG_TIMESTAMP"),
                                    Compare.Operator.GREATER_THAN_EQUAL,
                                    Expression.variable(lowerGranularityAttributes.get(i))),
                            reducedExpression
                    );
                }

                TimePeriod.Duration duration = this.incrementalDurations.get(i);
                String tableName = aggregationName + "_" + duration.toString();
                CompiledCondition compiledCondition = tableMap.get(tableName)
                        .compileCondition(lowerGranularity, metaInfoHolderForTableLookups, siddhiAppContext,
                                startEndExpressionExecutorList, tableMap, queryName);
                withinTableLowerGranularityCompileCondition.put(duration, compiledCondition);
            }
        }

        QueryParserHelper.reduceMetaComplexEvent(metaInfoHolderForTableLookups.getMetaStateEvent());

        // On compile condition.
        // After finding all the aggregates belonging to within duration, the final on condition (given as
        // "on stream1.name == aggregator.nickName ..." in the join query) must be executed on that data.
        // This condition is used for that purpose.
        onCompiledCondition = OperatorParser.constructOperator(new ComplexEventChunk<>(true), expression,
                matchingMetaInfoHolder, siddhiAppContext, variableExpressionExecutors, tableMap, queryName);

        return new IncrementalAggregateCompileCondition(aggregationName, isProcessingOnExternalTime, isDistributed,
                incrementalDurations, aggregationTables, getExpressionExecutorClones(outputExpressionExecutors),
                withinTableCompiledConditions, withinInMemoryCompileCondition,
                withinTableLowerGranularityCompileCondition, onCompiledCondition, additionalAttributes,
                perExpressionExecutor, startTimeEndTimeExpressionExecutor, timestampFilterExecutors,
                aggregateMetaSteamEvent, matchingMetaInfoHolder, metaInfoHolderForTableLookups,
                startEndExpressionExecutorList);
    }

    public void startPurging() {
        incrementalDataPurger.executeIncrementalDataPurging();
    }

    public void initialiseIncrementalExecutors(boolean isFirstEventArrived) {
        // State only updated when first event arrives to IncrementalAggregationProcessor
        if (isFirstEventArrived) {
            this.isFirstEventArrived = true;
            for (Map.Entry<TimePeriod.Duration, IncrementalExecutor> durationIncrementalExecutorEntry :
                    this.incrementalExecutorMap.entrySet()) {
                durationIncrementalExecutorEntry.getValue().setProcessingExecutor(true);
            }
        }
        this.incrementalExecutorsInitialiser.initialiseExecutors();
    }

    public void processEvents(ComplexEventChunk<StreamEvent> streamEventComplexEventChunk) {
        incrementalExecutorMap.get(incrementalDurations.get(0)).execute(streamEventComplexEventChunk);
    }
}

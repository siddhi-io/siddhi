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

package io.siddhi.core.aggregation;

import io.siddhi.core.aggregation.persistedaggregation.PersistedIncrementalExecutor;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.QueryableRecordTableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.IncrementalAggregateCompileCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.MemoryCalculable;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.constant.BoolConstant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.AGG_EXTERNAL_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_LAST_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_SHARD_ID_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.UNKNOWN_STATE;
import static io.siddhi.query.api.expression.Expression.Time.normalizeDuration;

/**
 * Aggregation runtime managing aggregation operations for aggregation definition.
 */
public class AggregationRuntime implements MemoryCalculable {
    private static final Logger LOG = LogManager.getLogger(AggregationRuntime.class);

    private AggregationDefinition aggregationDefinition;
    private boolean isProcessingOnExternalTime;
    private boolean isDistributed;
    private List<TimePeriod.Duration> incrementalDurations;
    private List<TimePeriod.Duration> activeIncrementalDurations;
    private Map<TimePeriod.Duration, Executor> incrementalExecutorMap;
    private Map<TimePeriod.Duration, Table> aggregationTables;
    private List<String> tableAttributesNameList;
    private MetaStreamEvent aggregateMetaSteamEvent;
    private List<ExpressionExecutor> outputExpressionExecutors;
    private Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap;
    private ExpressionExecutor shouldUpdateTimestamp;
    private Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap;
    private boolean isOptimisedLookup;
    private List<OutputAttribute> defaultSelectorList;
    private List<String> groupByVariablesList;
    private boolean isLatestEventColAdded;
    private int baseAggregatorBeginIndex;
    private List<Expression> finalBaseExpressionsList;

    private IncrementalDataPurger incrementalDataPurger;
    private IncrementalExecutorsInitialiser incrementalExecutorsInitialiser;

    private SingleStreamRuntime singleStreamRuntime;

    private LatencyTracker latencyTrackerFind;
    private ThroughputTracker throughputTrackerFind;

    private boolean isFirstEventArrived;
    private String timeZone;
    private Map<String, Map<TimePeriod.Duration, Executor>> aggregationDurationExecutorMap;

    public AggregationRuntime(AggregationDefinition aggregationDefinition, boolean isProcessingOnExternalTime,
                              boolean isDistributed, List<TimePeriod.Duration> aggregationDurations,
                              Map<TimePeriod.Duration, Executor> incrementalExecutorMap,
                              Map<TimePeriod.Duration, Table> aggregationTables,
                              List<ExpressionExecutor> outputExpressionExecutors,
                              Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap,
                              ExpressionExecutor shouldUpdateTimestamp,
                              Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap,
                              boolean isOptimisedLookup, List<OutputAttribute> defaultSelectorList,
                              List<String> groupByVariablesList,
                              boolean isLatestEventColAdded, int baseAggregatorBeginIndex,
                              List<Expression> finalBaseExpressionList, IncrementalDataPurger incrementalDataPurger,
                              IncrementalExecutorsInitialiser incrementalExecutorInitialiser,
                              SingleStreamRuntime singleStreamRuntime, MetaStreamEvent tableMetaStreamEvent,
                              LatencyTracker latencyTrackerFind, ThroughputTracker throughputTrackerFind,
                              String timeZone) {
        this.timeZone = timeZone;
        this.aggregationDefinition = aggregationDefinition;
        this.isProcessingOnExternalTime = isProcessingOnExternalTime;
        this.isDistributed = isDistributed;
        this.incrementalDurations = aggregationDurations;
        this.activeIncrementalDurations = aggregationDurations;
        this.incrementalExecutorMap = incrementalExecutorMap;
        this.aggregationTables = aggregationTables;
        this.tableAttributesNameList = tableMetaStreamEvent.getInputDefinitions().get(0).getAttributeList()
                .stream().map(Attribute::getName).collect(Collectors.toList());
        this.outputExpressionExecutors = outputExpressionExecutors;
        this.aggregateProcessingExecutorsMap = aggregateProcessingExecutorsMap;
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;
        this.groupByKeyGeneratorMap = groupByKeyGeneratorMap;
        this.isOptimisedLookup = isOptimisedLookup;
        this.defaultSelectorList = defaultSelectorList;
        this.groupByVariablesList = groupByVariablesList;
        this.isLatestEventColAdded = isLatestEventColAdded;
        this.baseAggregatorBeginIndex = baseAggregatorBeginIndex;
        this.finalBaseExpressionsList = finalBaseExpressionList;

        this.incrementalDataPurger = incrementalDataPurger;
        this.incrementalExecutorsInitialiser = incrementalExecutorInitialiser;

        this.singleStreamRuntime = singleStreamRuntime;
        this.aggregateMetaSteamEvent = new MetaStreamEvent();
        aggregationDefinition.getAttributeList().forEach(this.aggregateMetaSteamEvent::addOutputData);

        this.latencyTrackerFind = latencyTrackerFind;
        this.throughputTrackerFind = throughputTrackerFind;
        this.aggregationDurationExecutorMap = new HashMap<>();
        this.aggregationDurationExecutorMap.put(aggregationDefinition.getId(), incrementalExecutorMap);

    }

    private static void initMetaStreamEvent(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                            String inputReferenceId) {
        metaStreamEvent.addInputDefinition(inputDefinition);
        metaStreamEvent.setInputReferenceId(inputReferenceId);
        metaStreamEvent.initializeOnAfterWindowData();
        inputDefinition.getAttributeList().forEach(metaStreamEvent::addData);
    }

    private static MetaStreamEvent alterMetaStreamEvent(boolean isOnDemandQuery,
                                                        MetaStreamEvent originalMetaStreamEvent,
                                                        List<Attribute> additionalAttributes) {

        StreamDefinition alteredStreamDef = new StreamDefinition();
        String inputReferenceId = originalMetaStreamEvent.getInputReferenceId();

        if (!isOnDemandQuery) {
            for (Attribute attribute : originalMetaStreamEvent.getLastInputDefinition().getAttributeList()) {
                alteredStreamDef.attribute(attribute.getName(), attribute.getType());
            }
            if (inputReferenceId == null) {
                alteredStreamDef.setId(originalMetaStreamEvent.getLastInputDefinition().getId());
            }
        } else {
            // If it is on-demand query, no original join stream
            alteredStreamDef.setId("OnDemandQueryStream");
        }

        additionalAttributes.forEach(attribute -> alteredStreamDef.attribute(attribute.getName(), attribute.getType()));

        initMetaStreamEvent(originalMetaStreamEvent, alteredStreamDef, inputReferenceId);
        return originalMetaStreamEvent;
    }

    private static MetaStreamEvent createMetaStoreEvent(AbstractDefinition tableDefinition, String referenceId) {
        MetaStreamEvent metaStreamEventForTable = new MetaStreamEvent();
        metaStreamEventForTable.setEventType(MetaStreamEvent.EventType.TABLE);
        initMetaStreamEvent(metaStreamEventForTable, tableDefinition, referenceId);
        return metaStreamEventForTable;
    }

    private static MatchingMetaInfoHolder alterMetaInfoHolderForOnDemandQuery(
            MetaStreamEvent newMetaStreamEventWithStartEnd, MatchingMetaInfoHolder matchingMetaInfoHolder) {

        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        MetaStreamEvent incomingMetaStreamEvent = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0);
        metaStateEvent.addEvent(newMetaStreamEventWithStartEnd);
        metaStateEvent.addEvent(incomingMetaStreamEvent);

        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1,
                newMetaStreamEventWithStartEnd.getLastInputDefinition(),
                incomingMetaStreamEvent.getLastInputDefinition(), UNKNOWN_STATE);
    }

    private static MatchingMetaInfoHolder createNewStreamTableMetaInfoHolder(MetaStreamEvent metaStreamEvent,
                                                                             MetaStreamEvent metaStoreEvent) {

        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        metaStateEvent.addEvent(metaStreamEvent);
        metaStateEvent.addEvent(metaStoreEvent);
        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1,
                metaStreamEvent.getLastInputDefinition(), metaStoreEvent.getLastInputDefinition(), UNKNOWN_STATE);
    }

    private static List<OutputAttribute> constructSelectorList(boolean isProcessingOnExternalTime,
                                                               boolean isDistributed,
                                                               boolean isLatestEventColAdded,
                                                               int baseAggregatorBeginIndex,
                                                               int numGroupByVariables,
                                                               List<Expression> finalBaseExpressions,
                                                               AbstractDefinition incomingOutputStreamDefinition,
                                                               List<Variable> newGroupByList) {

        List<OutputAttribute> selectorList = new ArrayList<>();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();

        List<String> queryGroupByNames = newGroupByList.stream()
                .map(Variable::getAttributeName).collect(Collectors.toList());
        Variable maxVariable;
        if (!isProcessingOnExternalTime) {
            maxVariable = new Variable(AGG_START_TIMESTAMP_COL);
        } else if (isLatestEventColAdded) {
            maxVariable = new Variable(AGG_LAST_TIMESTAMP_COL);
        } else {
            maxVariable = new Variable(AGG_EXTERNAL_TIMESTAMP_COL);
        }

        int i = 0;
        //Add timestamp selector
        OutputAttribute timestampAttribute;
        if (!isProcessingOnExternalTime && queryGroupByNames.contains(AGG_START_TIMESTAMP_COL)) {
            timestampAttribute = new OutputAttribute(new Variable(AGG_START_TIMESTAMP_COL));
        } else {
            timestampAttribute = new OutputAttribute(attributeList.get(i).getName(),
                    Expression.function("max", new Variable(AGG_START_TIMESTAMP_COL)));
        }
        selectorList.add(timestampAttribute);
        i++;

        if (isDistributed) {
            selectorList.add(new OutputAttribute(AGG_SHARD_ID_COL, Expression.function("max",
                    new Variable(AGG_SHARD_ID_COL))));
            i++;
        }

        if (isProcessingOnExternalTime) {
            OutputAttribute externalTimestampAttribute;
            if (queryGroupByNames.contains(AGG_START_TIMESTAMP_COL)) {
                externalTimestampAttribute = new OutputAttribute(new Variable(AGG_EXTERNAL_TIMESTAMP_COL));
            } else {
                externalTimestampAttribute = new OutputAttribute(attributeList.get(i).getName(),
                        Expression.function("max", new Variable(AGG_EXTERNAL_TIMESTAMP_COL)));
            }
            selectorList.add(externalTimestampAttribute);
            i++;
        }

        for (int j = 0; j < numGroupByVariables; j++) {
            OutputAttribute groupByAttribute;
            Variable variable = new Variable(attributeList.get(i).getName());
            if (queryGroupByNames.contains(variable.getAttributeName())) {
                groupByAttribute = new OutputAttribute(variable);
            } else {
                groupByAttribute = new OutputAttribute(variable.getAttributeName(),
                        Expression.function("incrementalAggregator", "last",
                                new Variable(attributeList.get(i).getName()), maxVariable));
            }
            selectorList.add(groupByAttribute);
            i++;
        }

        if (isLatestEventColAdded) {
            baseAggregatorBeginIndex = baseAggregatorBeginIndex - 1;
        }

        for (; i < baseAggregatorBeginIndex; i++) {
            OutputAttribute outputAttribute;
            Variable variable = new Variable(attributeList.get(i).getName());
            if (queryGroupByNames.contains(variable.getAttributeName())) {
                outputAttribute = new OutputAttribute(variable);
            } else {
                outputAttribute = new OutputAttribute(attributeList.get(i).getName(),
                        Expression.function("incrementalAggregator", "last",
                                new Variable(attributeList.get(i).getName()), maxVariable));
            }
            selectorList.add(outputAttribute);
        }

        if (isLatestEventColAdded) {
            OutputAttribute lastTimestampAttribute = new OutputAttribute(AGG_LAST_TIMESTAMP_COL,
                    Expression.function("max", new Variable(AGG_LAST_TIMESTAMP_COL)));
            selectorList.add(lastTimestampAttribute);
            i++;
        }

        for (Expression finalBaseExpression : finalBaseExpressions) {
            OutputAttribute outputAttribute = new OutputAttribute(attributeList.get(i).getName(), finalBaseExpression);
            selectorList.add(outputAttribute);
            i++;
        }

        return selectorList;
    }

    public AggregationDefinition getAggregationDefinition() {
        return aggregationDefinition;
    }

    public SingleStreamRuntime getSingleStreamRuntime() {
        return singleStreamRuntime;
    }

    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            SiddhiQueryContext siddhiQueryContext) {
        try {
            SnapshotService.getSkipStateStorageThreadLocal().set(true);
            if (latencyTrackerFind != null &&
                    Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0) {
                latencyTrackerFind.markIn();
                throughputTrackerFind.eventIn();
            }
            if (!isDistributed && !isFirstEventArrived) {
                // No need to initialise executors if it is distributed
                initialiseExecutors(false);
            }
            return ((IncrementalAggregateCompileCondition) compiledCondition).find(matchingEvent,
                    incrementalExecutorMap, aggregateProcessingExecutorsMap, groupByKeyGeneratorMap,
                    shouldUpdateTimestamp, timeZone);

        } finally {
            SnapshotService.getSkipStateStorageThreadLocal().set(null);
            if (latencyTrackerFind != null &&
                    Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0) {
                latencyTrackerFind.markOut();
            }
        }
    }

    public CompiledCondition compileExpression(Expression expression, Within within, Expression per,
                                               List<Variable> queryGroupByList,
                                               MatchingMetaInfoHolder matchingMetaInfoHolder,
                                               List<VariableExpressionExecutor> variableExpressionExecutors,
                                               Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {

        String aggregationName = aggregationDefinition.getId();
        boolean isOptimisedTableLookup = isOptimisedLookup;
        Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions = new HashMap<>();
        CompiledCondition withinInMemoryCompileCondition;
        CompiledCondition onCompiledCondition;
        List<Attribute> additionalAttributes = new ArrayList<>();

        // Define additional attribute list
        additionalAttributes.add(new Attribute("_START", Attribute.Type.LONG));
        additionalAttributes.add(new Attribute("_END", Attribute.Type.LONG));

        int lowerGranularitySize = this.activeIncrementalDurations.size() - 1;
        List<String> lowerGranularityAttributes = new ArrayList<>();
        if (isDistributed) {
            //Add additional attributes to get base aggregation timestamps based on current timestamps
            // for values calculated in in-memory in the shards
            for (int i = 0; i < lowerGranularitySize; i++) {
                String attributeName = "_AGG_TIMESTAMP_FILTER_" + i;
                additionalAttributes.add(new Attribute(attributeName, Attribute.Type.LONG));
                lowerGranularityAttributes.add(attributeName);
            }
        }

        // Get table definition. Table definitions for all the tables used to persist aggregates are similar.
        // Therefore it's enough to get the definition from one table.
        AbstractDefinition tableDefinition = aggregationTables.get(activeIncrementalDurations.get(0)).
                getTableDefinition();

        boolean isOnDemandQuery = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvents().length == 1;

        // Alter existing meta stream event or create new one if a meta stream doesn't exist
        // After calling this method the original MatchingMetaInfoHolder's meta stream event would be altered
        // Alter meta info holder to contain stream event and aggregate both when it's a on-demand query
        MetaStreamEvent metaStreamEventForTableLookups;
        if (isOnDemandQuery) {
            metaStreamEventForTableLookups = alterMetaStreamEvent(true, new MetaStreamEvent(), additionalAttributes);
            matchingMetaInfoHolder = alterMetaInfoHolderForOnDemandQuery(metaStreamEventForTableLookups,
                    matchingMetaInfoHolder);
        } else {
            metaStreamEventForTableLookups = alterMetaStreamEvent(false,
                    matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(0), additionalAttributes);
        }


        // Create new MatchingMetaInfoHolder containing newMetaStreamEventWithStartEnd and table meta event
        String aggReferenceId = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(1).getInputReferenceId();
        String referenceName = aggReferenceId == null ? aggregationName : aggReferenceId;

        MetaStreamEvent metaStoreEventForTableLookups = createMetaStoreEvent(tableDefinition, referenceName);

        // Create new MatchingMetaInfoHolder containing metaStreamEventForTableLookups and table meta event
        MatchingMetaInfoHolder metaInfoHolderForTableLookups = createNewStreamTableMetaInfoHolder(
                metaStreamEventForTableLookups, metaStoreEventForTableLookups);

        // Create per expression executor
        ExpressionExecutor perExpressionExecutor;
        if (per != null) {
            perExpressionExecutor = ExpressionParser.parseExpression(per, matchingMetaInfoHolder.getMetaStateEvent(),
                    matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            if (perExpressionExecutor.getReturnType() != Attribute.Type.STRING) {
                throw new SiddhiAppCreationException(
                        "Query " + siddhiQueryContext.getName() + "'s per value expected a string but found "
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
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
        } else {
            throw new SiddhiAppCreationException("Syntax Error : Aggregation read query must contain a `within` " +
                    "definition for filtering of aggregation data.");
        }

        // Create within expression
        Expression timeFilterExpression;
        if (isProcessingOnExternalTime) {
            timeFilterExpression = Expression.variable(AGG_EXTERNAL_TIMESTAMP_COL);
        } else {
            timeFilterExpression = Expression.variable(AGG_START_TIMESTAMP_COL);
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
                        Expression.value(this.activeIncrementalDurations.get(i + 1).toString())};
                Expression filterExpression = new AttributeFunction("incrementalAggregator",
                        "getAggregationStartTime", expressionArray);
                timestampFilterExecutors.add(ExpressionParser.parseExpression(filterExpression,
                        matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                        variableExpressionExecutors, false, 0,
                        ProcessingMode.BATCH, false, siddhiQueryContext));
            }
        }

        // Create compile condition per each table used to persist aggregates.
        // These compile conditions are used to check whether the aggregates in tables are within the given duration.
        // Combine with and on condition for table query
        boolean shouldApplyReducedCondition = false;
        Expression reducedExpression = null;

        //Check if there is no on conditions
        if (!(expression instanceof BoolConstant)) {
            // For abstract queryable table
            AggregationExpressionBuilder aggregationExpressionBuilder = new AggregationExpressionBuilder(expression);
            AggregationExpressionVisitor expressionVisitor = new AggregationExpressionVisitor(
                    metaStreamEventForTableLookups.getInputReferenceId(),
                    metaStreamEventForTableLookups.getLastInputDefinition().getAttributeList(),
                    this.tableAttributesNameList
            );
            aggregationExpressionBuilder.build(expressionVisitor);
            shouldApplyReducedCondition = expressionVisitor.applyReducedExpression();
            reducedExpression = expressionVisitor.getReducedExpression();
        }

        Expression withinExpressionTable;
        if (shouldApplyReducedCondition) {
            withinExpressionTable = Expression.and(withinExpression, reducedExpression);
        } else {
            withinExpressionTable = withinExpression;
        }

        List<Variable> queryGroupByListCopy = new ArrayList<>(queryGroupByList);

        Variable timestampVariable = new Variable(AGG_START_TIMESTAMP_COL);
        List<String> queryGroupByNamesList = queryGroupByListCopy.stream()
                .map(Variable::getAttributeName)
                .collect(Collectors.toList());
        boolean queryGroupByContainsTimestamp = queryGroupByNamesList.remove(AGG_START_TIMESTAMP_COL);

        boolean isQueryGroupBySameAsAggGroupBy =
                queryGroupByListCopy.isEmpty() ||
                        (queryGroupByListCopy.contains(timestampVariable) &&
                                queryGroupByNamesList.equals(groupByVariablesList));

        List<VariableExpressionExecutor> variableExpExecutorsForTableLookups = new ArrayList<>();

        Map<TimePeriod.Duration, CompiledSelection> withinTableCompiledSelection = new HashMap<>();
        if (isOptimisedTableLookup) {
            Selector selector = Selector.selector();

            List<Variable> groupByList = new ArrayList<>();
            if (!isQueryGroupBySameAsAggGroupBy) {
                if (queryGroupByContainsTimestamp) {
                    if (isProcessingOnExternalTime) {
                        groupByList.add(new Variable(AGG_EXTERNAL_TIMESTAMP_COL));
                    } else {
                        groupByList.add(new Variable(AGG_START_TIMESTAMP_COL));
                    }
                    //Remove timestamp to process the rest
                    queryGroupByListCopy.remove(timestampVariable);
                }
                for (Variable queryGroupBy : queryGroupByListCopy) {
                    String referenceId = queryGroupBy.getStreamId();
                    if (referenceId == null) {
                        if (tableAttributesNameList.contains(queryGroupBy.getAttributeName())) {
                            groupByList.add(queryGroupBy);
                        }
                    } else if (referenceId.equalsIgnoreCase(referenceName)) {
                        groupByList.add(queryGroupBy);
                    }
                }
                // If query group bys are based on joining stream
                if (groupByList.isEmpty()) {
                    isQueryGroupBySameAsAggGroupBy = true;
                }
            }

            groupByList.forEach((groupBy) -> groupBy.setStreamId(referenceName));

            selector.addGroupByList(groupByList);

            List<OutputAttribute> selectorList;
            if (!isQueryGroupBySameAsAggGroupBy) {
                selectorList = constructSelectorList(isProcessingOnExternalTime, isDistributed, isLatestEventColAdded,
                        baseAggregatorBeginIndex, groupByVariablesList.size(), finalBaseExpressionsList,
                        tableDefinition, groupByList);
            } else {
                selectorList = defaultSelectorList;
            }

            for (OutputAttribute outputAttribute : selectorList) {
                if (outputAttribute.getExpression() instanceof Variable) {
                    ((Variable) outputAttribute.getExpression()).setStreamId(referenceName);
                } else {
                    for (Expression parameter :
                            ((AttributeFunction) outputAttribute.getExpression()).getParameters()) {
                        ((Variable) parameter).setStreamId(referenceName);
                    }
                }
            }
            selector.addSelectionList(selectorList);

            try {
                aggregationTables.entrySet().forEach(
                        (durationTableEntry -> {
                            CompiledSelection compiledSelection = ((QueryableProcessor) durationTableEntry.getValue())
                                    .compileSelection(
                                            selector, tableDefinition.getAttributeList(), metaInfoHolderForTableLookups,
                                            variableExpExecutorsForTableLookups, tableMap, siddhiQueryContext
                                    );
                            withinTableCompiledSelection.put(durationTableEntry.getKey(), compiledSelection);
                        })
                );
            } catch (SiddhiAppCreationException | SiddhiAppValidationException | QueryableRecordTableException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Aggregation Query optimization failed for aggregation: '" + aggregationName + "'. " +
                            "Creating table lookup query in normal mode. Reason for failure: " + e.getMessage(), e);
                }
                isOptimisedTableLookup = false;
            }

        }

        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            CompiledCondition withinTableCompileCondition = entry.getValue().compileCondition(withinExpressionTable,
                    metaInfoHolderForTableLookups, variableExpExecutorsForTableLookups, tableMap,
                    siddhiQueryContext);
            withinTableCompiledConditions.put(entry.getKey(), withinTableCompileCondition);
        }

        // Create compile condition for in-memory data.
        // This compile condition is used to check whether the running aggregates (in-memory data)
        // are within given duration
        withinInMemoryCompileCondition = OperatorParser.constructOperator(new ComplexEventChunk<>(),
                withinExpression, metaInfoHolderForTableLookups, variableExpExecutorsForTableLookups, tableMap,
                siddhiQueryContext);

        // Create compile condition for in-memory data, in case of distributed
        // Look at the lower level granularities
        Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition = new HashMap<>();
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
                    if (shouldApplyReducedCondition) {
                        lowerGranularity = Expression.and(
                                Expression.compare(
                                        Expression.variable("AGG_TIMESTAMP"),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable(lowerGranularityAttributes.get(i))),
                                reducedExpression
                        );
                    } else {
                        lowerGranularity =
                                Expression.compare(
                                        Expression.variable("AGG_TIMESTAMP"),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable(lowerGranularityAttributes.get(i)));
                    }
                }
                TimePeriod.Duration duration = this.activeIncrementalDurations.get(i);
                String tableName = aggregationName + "_" + duration.toString();
                CompiledCondition compiledCondition = tableMap.get(tableName).compileCondition(lowerGranularity,
                        metaInfoHolderForTableLookups, variableExpExecutorsForTableLookups, tableMap,
                        siddhiQueryContext);
                withinTableLowerGranularityCompileCondition.put(duration, compiledCondition);
            }
        }

        QueryParserHelper.reduceMetaComplexEvent(metaInfoHolderForTableLookups.getMetaStateEvent());

        // On compile condition.
        // After finding all the aggregates belonging to within duration, the final on condition (given as
        // "on stream1.name == aggregator.nickName ..." in the join query) must be executed on that data.
        // This condition is used for that purpose.
        onCompiledCondition = OperatorParser.constructOperator(new ComplexEventChunk<>(), expression,
                matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);

        return new IncrementalAggregateCompileCondition(isOnDemandQuery, aggregationName, isProcessingOnExternalTime,
                isDistributed, activeIncrementalDurations, aggregationTables, outputExpressionExecutors,
                isOptimisedTableLookup, withinTableCompiledSelection, withinTableCompiledConditions,
                withinInMemoryCompileCondition, withinTableLowerGranularityCompileCondition, onCompiledCondition,
                additionalAttributes, perExpressionExecutor, startTimeEndTimeExpressionExecutor,
                timestampFilterExecutors, aggregateMetaSteamEvent, matchingMetaInfoHolder,
                metaInfoHolderForTableLookups, variableExpExecutorsForTableLookups);

    }

    public void startPurging() {
        incrementalDataPurger.executeIncrementalDataPurging();
    }

    public void initialiseExecutors(boolean isFirstEventArrived) {
        // State only updated when first event arrives to IncrementalAggregationProcessor
        if (isFirstEventArrived) {
            this.isFirstEventArrived = true;
            for (Map.Entry<TimePeriod.Duration, Executor> durationIncrementalExecutorEntry :
                    this.incrementalExecutorMap.entrySet()) {
                if (activeIncrementalDurations.contains(durationIncrementalExecutorEntry.getKey())) {
                    if (durationIncrementalExecutorEntry.getValue() instanceof IncrementalExecutor) {
                        ((IncrementalExecutor) durationIncrementalExecutorEntry.getValue()).setProcessingExecutor(true);
                    } else {
                        ((PersistedIncrementalExecutor) durationIncrementalExecutorEntry.getValue())
                                .setProcessingExecutor(true);
                    }
                }
            }
        }
        this.incrementalExecutorsInitialiser.initialiseExecutors();
    }

    public void processEvents(ComplexEventChunk<StreamEvent> streamEventComplexEventChunk) {
        incrementalExecutorMap.get(incrementalDurations.get(0)).execute(streamEventComplexEventChunk);
    }

    public Map<String, Map<TimePeriod.Duration, Executor>> getAggregationDurationExecutorMap() {
        return aggregationDurationExecutorMap;
    }

    public void setAggregationDurationExecutorMap(Map<String,
            Map<TimePeriod.Duration, Executor>> aggregationDurationExecutorMap) {
        this.aggregationDurationExecutorMap = aggregationDurationExecutorMap;
    }
}

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
import io.siddhi.core.aggregation.IncrementalAggregationProcessor;
import io.siddhi.core.aggregation.IncrementalDataPurger;
import io.siddhi.core.aggregation.IncrementalExecutor;
import io.siddhi.core.aggregation.IncrementalExecutorsInitialiser;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.single.EntryValveExecutor;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.query.selector.attribute.aggregator.incremental.IncrementalAttributeAggregator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.SiddhiClassLoader;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigManager;
import io.siddhi.core.util.extension.holder.FunctionExecutorExtensionHolder;
import io.siddhi.core.util.extension.holder.IncrementalAttributeAggregatorExtensionHolder;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.constant.StringConstant;
import io.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.AGG_EXTERNAL_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_LAST_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_SHARD_ID_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_PARTITION_BY_ID;
import static io.siddhi.core.util.SiddhiConstants.METRIC_INFIX_AGGREGATIONS;
import static io.siddhi.core.util.SiddhiConstants.METRIC_TYPE_FIND;
import static io.siddhi.core.util.SiddhiConstants.METRIC_TYPE_INSERT;

/**
 * This is the parser class of incremental aggregation definition.
 */
public class AggregationParser {

    public static AggregationRuntime parse(AggregationDefinition aggregationDefinition,
                                           SiddhiAppContext siddhiAppContext,
                                           Map<String, AbstractDefinition> streamDefinitionMap,
                                           Map<String, AbstractDefinition> tableDefinitionMap,
                                           Map<String, AbstractDefinition> windowDefinitionMap,
                                           Map<String, AbstractDefinition> aggregationDefinitionMap,
                                           Map<String, Table> tableMap,
                                           Map<String, Window> windowMap,
                                           Map<String, AggregationRuntime> aggregationMap,
                                           SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder) {

        if (aggregationDefinition == null) {
            throw new SiddhiAppCreationException(
                    "AggregationDefinition instance is null. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'");
        }
        if (aggregationDefinition.getTimePeriod() == null) {
            throw new SiddhiAppCreationException(
                    "AggregationDefinition '" + aggregationDefinition.getId() + "'s timePeriod is null. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (aggregationDefinition.getSelector() == null) {
            throw new SiddhiAppCreationException(
                    "AggregationDefinition '" + aggregationDefinition.getId() + "'s selection is not defined. " +
                            "Hence, can't create the siddhi app '" + siddhiAppContext.getName() + "'",
                    aggregationDefinition.getQueryContextStartIndex(), aggregationDefinition.getQueryContextEndIndex());
        }
        if (streamDefinitionMap.get(aggregationDefinition.getBasicSingleInputStream().getStreamId()) == null) {
            throw new SiddhiAppCreationException("Stream " + aggregationDefinition.getBasicSingleInputStream().
                    getStreamId() + " has not been defined");
        }

        Element userDefinedPrimaryKey = AnnotationHelper.getAnnotationElement(
                SiddhiConstants.ANNOTATION_PRIMARY_KEY, null, aggregationDefinition.getAnnotations());
        if (userDefinedPrimaryKey != null) {
            throw new SiddhiAppCreationException("Aggregation Tables have predefined primary key, but found '" +
                    userDefinedPrimaryKey.getValue() + "' primary key defined though annotation.");
        }

        try {
            List<VariableExpressionExecutor> incomingVariableExpressionExecutors = new ArrayList<>();

            String aggregatorName = aggregationDefinition.getId();
            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, aggregatorName);

            StreamRuntime streamRuntime = InputStreamParser.parse(aggregationDefinition.getBasicSingleInputStream(),
                    null, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, aggregationDefinitionMap,
                    tableMap, windowMap, aggregationMap, incomingVariableExpressionExecutors, false,
                    siddhiQueryContext);

            // Get original meta for later use.
            MetaStreamEvent incomingMetaStreamEvent = (MetaStreamEvent) streamRuntime.getMetaComplexEvent();
            // Create new meta stream event.
            // This must hold the timestamp, group by attributes (if given) and the incremental attributes, in
            // onAfterWindowData array
            // Example format: AGG_TIMESTAMP, groupByAttribute1, groupByAttribute2, AGG_incAttribute1, AGG_incAttribute2
            // AGG_incAttribute1, AGG_incAttribute2 would have the same attribute names as in
            // finalListOfIncrementalAttributes
            incomingMetaStreamEvent.initializeAfterWindowData(); // To enter data as onAfterWindowData


            List<TimePeriod.Duration> incrementalDurations = getSortedPeriods(aggregationDefinition.getTimePeriod());

            //Incoming executors will be executors for timestamp, externalTimestamp(if used),
            // group by attributes (if given) and the incremental attributes expression executors
            List<ExpressionExecutor> incomingExpressionExecutors = new ArrayList<>();
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators = new ArrayList<>();
            List<Variable> groupByVariableList = aggregationDefinition.getSelector().getGroupByList();
            List<Expression> outputExpressions = new ArrayList<>(); //Expressions to get
            // final aggregate outputs. e.g avg = sum/count

            boolean isProcessingOnExternalTime = aggregationDefinition.getAggregateAttribute() != null;
            boolean isGroupBy = aggregationDefinition.getSelector().getGroupByList().size() != 0;


            final boolean isDistributed;
            ConfigManager configManager = siddhiAppContext.getSiddhiContext().getConfigManager();
            final String shardId = configManager.extractProperty("shardId");
            boolean enablePartitioning = false;
            Annotation partitionById = AnnotationHelper.getAnnotation(ANNOTATION_PARTITION_BY_ID,
                    aggregationDefinition.getAnnotations());
            if (partitionById != null) {
                String enableElement = partitionById.getElement("enable");
                enablePartitioning = enableElement == null || Boolean.parseBoolean(enableElement);
            }

            boolean shouldPartitionById = Boolean.parseBoolean(configManager.extractProperty("partitionById"));

            if (enablePartitioning || shouldPartitionById) {
                if (shardId == null) {
                    throw new SiddhiAppCreationException("Configuration 'shardId' not provided for @partitionById " +
                            "annotation");
                }
                isDistributed = true;
            } else {
                isDistributed = false;
            }

            populateIncomingAggregatorsAndExecutors(aggregationDefinition, siddhiQueryContext, tableMap,
                    incomingVariableExpressionExecutors, incomingMetaStreamEvent, incomingExpressionExecutors,
                    incrementalAttributeAggregators, groupByVariableList, outputExpressions, isProcessingOnExternalTime,
                    isDistributed, shardId);

            boolean isLatestEventColAdded = incomingMetaStreamEvent.getOutputData()
                    .get(incomingMetaStreamEvent.getOutputData().size() - 1)
                    .getName().equals(AGG_LAST_TIMESTAMP_COL);


            int baseAggregatorBeginIndex = incomingMetaStreamEvent.getOutputData().size();

            List<Expression> finalBaseExpressions = new ArrayList<>();
            populateFinalBaseAggregators(tableMap, incomingVariableExpressionExecutors, incomingMetaStreamEvent,
                    incomingExpressionExecutors, incrementalAttributeAggregators, siddhiQueryContext,
                    finalBaseExpressions);

            StreamDefinition incomingOutputStreamDefinition = StreamDefinition.id(aggregatorName + "_intermediate");
            incomingOutputStreamDefinition.setQueryContextStartIndex(aggregationDefinition.getQueryContextStartIndex());
            incomingOutputStreamDefinition.setQueryContextEndIndex(aggregationDefinition.getQueryContextEndIndex());
            MetaStreamEvent processedMetaStreamEvent = new MetaStreamEvent();
            for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                incomingOutputStreamDefinition.attribute(attribute.getName(), attribute.getType());
                processedMetaStreamEvent.addOutputData(attribute);
            }
            incomingMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.addInputDefinition(incomingOutputStreamDefinition);
            processedMetaStreamEvent.setOutputDefinition(incomingOutputStreamDefinition);

            // Executors of processing meta
            List<VariableExpressionExecutor> processVariableExpressionExecutors = new ArrayList<>();

            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap = new HashMap<>();
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMapForFind = new HashMap<>();

            incrementalDurations.forEach(
                    incrementalDuration -> {
                        processExpressionExecutorsMap.put(
                                incrementalDuration,
                                constructProcessExpressionExecutors(
                                        siddhiQueryContext, tableMap, baseAggregatorBeginIndex, finalBaseExpressions,
                                        incomingOutputStreamDefinition, processedMetaStreamEvent,
                                        processVariableExpressionExecutors, isProcessingOnExternalTime,
                                        incrementalDuration, isDistributed, shardId, isLatestEventColAdded));
                        processExpressionExecutorsMapForFind.put(
                                incrementalDuration,
                                constructProcessExpressionExecutors(
                                        siddhiQueryContext, tableMap, baseAggregatorBeginIndex, finalBaseExpressions,
                                        incomingOutputStreamDefinition, processedMetaStreamEvent,
                                        processVariableExpressionExecutors, isProcessingOnExternalTime,
                                        incrementalDuration, isDistributed, shardId, isLatestEventColAdded));

                    });

            ExpressionExecutor shouldUpdateTimestamp = null;
            if (isLatestEventColAdded) {
                Expression shouldUpdateTimestampExp = new Variable(AGG_LAST_TIMESTAMP_COL);
                shouldUpdateTimestamp = ExpressionParser.parseExpression(shouldUpdateTimestampExp,
                        processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors,
                        false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            }


            List<ExpressionExecutor> outputExpressionExecutors = outputExpressions.stream()
                    .map(expression -> ExpressionParser.parseExpression(expression, processedMetaStreamEvent, 0,
                            tableMap, processVariableExpressionExecutors, isGroupBy, 0, ProcessingMode.BATCH, false,
                            siddhiQueryContext))
                    .collect(Collectors.toList());

            // Create group by key generator
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap = new HashMap<>();
            incrementalDurations.forEach(
                    incrementalDuration -> {
                        GroupByKeyGenerator groupByKeyGenerator = null;
                        if (isProcessingOnExternalTime || isGroupBy) {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            if (isProcessingOnExternalTime) {
                                Expression externalTimestampExpression =
                                        AttributeFunction.function(
                                                "incrementalAggregator", "getAggregationStartTime",
                                                new Variable(AGG_EXTERNAL_TIMESTAMP_COL),
                                                new StringConstant(incrementalDuration.name())
                                        );
                                groupByExpressionList.add(externalTimestampExpression);
                            }
                            groupByExpressionList.addAll(groupByVariableList.stream()
                                    .map(groupByVariable -> (Expression) groupByVariable)
                                    .collect(Collectors.toList()));
                            groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList, processedMetaStreamEvent,
                                    SiddhiConstants.UNKNOWN_STATE, tableMap, processVariableExpressionExecutors,
                                    siddhiQueryContext);
                        }
                        groupByKeyGeneratorMap.put(incrementalDuration, groupByKeyGenerator);
                    }
            );

            // GroupBy for reading
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMapForReading = new HashMap<>();
            if (isDistributed && !isProcessingOnExternalTime) {
                incrementalDurations.forEach(
                        incrementalDuration -> {
                            List<Expression> groupByExpressionList = new ArrayList<>();
                            Expression timestampExpression =
                                    AttributeFunction.function(
                                            "incrementalAggregator", "getAggregationStartTime",
                                            new Variable(AGG_START_TIMESTAMP_COL),
                                            new StringConstant(incrementalDuration.name())
                                    );
                            groupByExpressionList.add(timestampExpression);
                            if (isGroupBy) {
                                groupByExpressionList.addAll(groupByVariableList.stream()
                                        .map(groupByVariable -> (Expression) groupByVariable)
                                        .collect(Collectors.toList()));
                            }
                            GroupByKeyGenerator groupByKeyGenerator = new GroupByKeyGenerator(groupByExpressionList,
                                    processedMetaStreamEvent, SiddhiConstants.UNKNOWN_STATE, tableMap,
                                    processVariableExpressionExecutors, siddhiQueryContext);
                            groupByKeyGeneratorMapForReading.put(incrementalDuration, groupByKeyGenerator);
                        }

                );

            } else {
                groupByKeyGeneratorMapForReading.putAll(groupByKeyGeneratorMap);
            }

            // Create new scheduler
            EntryValveExecutor entryValveExecutor = new EntryValveExecutor(siddhiAppContext);
            LockWrapper lockWrapper = new LockWrapper(aggregatorName);
            lockWrapper.setLock(new ReentrantLock());

            Scheduler scheduler = SchedulerParser.parse(entryValveExecutor, siddhiQueryContext);
            scheduler.init(lockWrapper, aggregatorName);
            scheduler.setStreamEventFactory(new StreamEventFactory(processedMetaStreamEvent));

            QueryParserHelper.reduceMetaComplexEvent(incomingMetaStreamEvent);
            QueryParserHelper.reduceMetaComplexEvent(processedMetaStreamEvent);
            QueryParserHelper.updateVariablePosition(incomingMetaStreamEvent, incomingVariableExpressionExecutors);
            QueryParserHelper.updateVariablePosition(processedMetaStreamEvent, processVariableExpressionExecutors);

            Map<TimePeriod.Duration, Table> aggregationTables = initDefaultTables(aggregatorName,
                    incrementalDurations, processedMetaStreamEvent.getOutputStreamDefinition(),
                    siddhiAppRuntimeBuilder, aggregationDefinition.getAnnotations(), groupByVariableList,
                    isProcessingOnExternalTime, isDistributed);

            Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap = buildIncrementalExecutors(
                    processedMetaStreamEvent, processExpressionExecutorsMap, groupByKeyGeneratorMap, incrementalDurations,
                    aggregationTables, siddhiQueryContext, aggregatorName, shouldUpdateTimestamp);

            boolean isOptimisedLookup = aggregationTables.get(incrementalDurations.get(0)) instanceof QueryableProcessor;

            List<String> groupByVariablesList = groupByVariableList.stream()
                    .map(Variable::getAttributeName)
                    .collect(Collectors.toList());

            List<OutputAttribute> defaultSelectorList = new ArrayList<>();
            if (isOptimisedLookup) {
                defaultSelectorList = incomingOutputStreamDefinition.getAttributeList().stream()
                        .map((attribute) -> new OutputAttribute(new Variable(attribute.getName())))
                        .collect(Collectors.toList());
            }

            IncrementalDataPurger incrementalDataPurger = new IncrementalDataPurger();
            incrementalDataPurger.init(aggregationDefinition, new StreamEventFactory(processedMetaStreamEvent)
                    , aggregationTables, isProcessingOnExternalTime, siddhiQueryContext);

            //Recreate in-memory data from tables
            IncrementalExecutorsInitialiser incrementalExecutorsInitialiser = new IncrementalExecutorsInitialiser(
                    incrementalDurations, aggregationTables, incrementalExecutorMap, isDistributed, shardId,
                    siddhiAppContext, processedMetaStreamEvent, tableMap, windowMap, aggregationMap);

            IncrementalExecutor rootIncrementalExecutor = incrementalExecutorMap.get(incrementalDurations.get(0));
            rootIncrementalExecutor.setScheduler(scheduler);
            // Connect entry valve to root incremental executor
            entryValveExecutor.setNextExecutor(rootIncrementalExecutor);

            QueryParserHelper.initStreamRuntime(streamRuntime, incomingMetaStreamEvent, lockWrapper, aggregatorName);

            LatencyTracker latencyTrackerFind = null;
            LatencyTracker latencyTrackerInsert = null;

            ThroughputTracker throughputTrackerFind = null;
            ThroughputTracker throughputTrackerInsert = null;

            if (siddhiAppContext.getStatisticsManager() != null) {
                latencyTrackerFind = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                latencyTrackerInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);

                throughputTrackerFind = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_FIND);
                throughputTrackerInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                        aggregationDefinition.getId(), METRIC_INFIX_AGGREGATIONS, METRIC_TYPE_INSERT);
            }

            AggregationRuntime aggregationRuntime = new AggregationRuntime(aggregationDefinition,
                    isProcessingOnExternalTime, isDistributed, incrementalDurations, incrementalExecutorMap,
                    aggregationTables, outputExpressionExecutors, processExpressionExecutorsMapForFind,
                    shouldUpdateTimestamp, groupByKeyGeneratorMapForReading, isOptimisedLookup, defaultSelectorList,
                    groupByVariablesList, isLatestEventColAdded, baseAggregatorBeginIndex,
                    finalBaseExpressions, incrementalDataPurger, incrementalExecutorsInitialiser,
                    ((SingleStreamRuntime) streamRuntime), processedMetaStreamEvent,
                    latencyTrackerFind, throughputTrackerFind);

            streamRuntime.setCommonProcessor(new IncrementalAggregationProcessor(aggregationRuntime,
                    incomingExpressionExecutors, processedMetaStreamEvent, latencyTrackerInsert,
                    throughputTrackerInsert, siddhiAppContext));

            return aggregationRuntime;

        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, aggregationDefinition, siddhiAppContext);
            throw t;
        }
    }

    private static Map<TimePeriod.Duration, IncrementalExecutor> buildIncrementalExecutors(
            MetaStreamEvent processedMetaStreamEvent,
            Map<TimePeriod.Duration, List<ExpressionExecutor>> processExpressionExecutorsMap,
            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList,
            List<TimePeriod.Duration> incrementalDurations,
            Map<TimePeriod.Duration, Table> aggregationTables,
            SiddhiQueryContext siddhiQueryContext,
            String aggregatorName, ExpressionExecutor shouldUpdateTimestamp) {

        Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap = new HashMap<>();
        // Create incremental executors
        IncrementalExecutor child;
        IncrementalExecutor root = null;
        for (int i = incrementalDurations.size() - 1; i >= 0; i--) {
            // Base incremental expression executors created using new meta
            boolean isRoot = false;
            if (i == 0) {
                isRoot = true;
            }
            child = root;
            TimePeriod.Duration duration = incrementalDurations.get(i);

            IncrementalExecutor incrementalExecutor = new IncrementalExecutor(aggregatorName, duration,
                    processExpressionExecutorsMap.get(duration), shouldUpdateTimestamp,
                    groupByKeyGeneratorList.get(duration), isRoot, aggregationTables.get(duration),
                    child, siddhiQueryContext, processedMetaStreamEvent);

            incrementalExecutorMap.put(duration, incrementalExecutor);
            root = incrementalExecutor;
        }
        return incrementalExecutorMap;
    }

    private static List<ExpressionExecutor> constructProcessExpressionExecutors(
            SiddhiQueryContext siddhiQueryContext, Map<String, Table> tableMap, int baseAggregatorBeginIndex,
            List<Expression> finalBaseExpressions, StreamDefinition incomingOutputStreamDefinition,
            MetaStreamEvent processedMetaStreamEvent,
            List<VariableExpressionExecutor> processVariableExpressionExecutors, boolean isProcessingOnExternalTime,
            TimePeriod.Duration duration, boolean isDistributed, String shardId, boolean isLatestEventColAdded) {

        List<ExpressionExecutor> processExpressionExecutors = new ArrayList<>();
        List<Attribute> attributeList = incomingOutputStreamDefinition.getAttributeList();

        int i = 1;
        //Add timestamp executor
        Attribute attribute = attributeList.get(0);
        VariableExpressionExecutor variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser
                .parseExpression(new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                        processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
        processExpressionExecutors.add(variableExpressionExecutor);

        if (isDistributed) {
            Expression shardIdExpression = Expression.value(shardId);
            ExpressionExecutor shardIdExpressionExecutor = ExpressionParser.parseExpression(shardIdExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, true, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(shardIdExpressionExecutor);
            i++;
        }

        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression =
                    AttributeFunction.function("incrementalAggregator", "getAggregationStartTime",
                            new Variable(AGG_EXTERNAL_TIMESTAMP_COL), new StringConstant(duration.name()));
            ExpressionExecutor externalTimestampExecutor = ExpressionParser.parseExpression(
                    externalTimestampExpression, processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(externalTimestampExecutor);
            i++;
        }

        if (isLatestEventColAdded) {
            baseAggregatorBeginIndex = baseAggregatorBeginIndex - 1;
        }

        for (; i < baseAggregatorBeginIndex; i++) {
            attribute = attributeList.get(i);
            variableExpressionExecutor = (VariableExpressionExecutor) ExpressionParser.parseExpression(
                    new Variable(attribute.getName()), processedMetaStreamEvent, 0, tableMap,
                    processVariableExpressionExecutors, true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(variableExpressionExecutor);
        }

        if (isLatestEventColAdded) {
            Expression lastTimestampExpression =
                    AttributeFunction.function("max", new Variable(AGG_LAST_TIMESTAMP_COL));
            ExpressionExecutor latestTimestampExecutor = ExpressionParser.parseExpression(lastTimestampExpression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors, true, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(latestTimestampExecutor);
        }

        for (Expression expression : finalBaseExpressions) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    processedMetaStreamEvent, 0, tableMap, processVariableExpressionExecutors,
                    true, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
            processExpressionExecutors.add(expressionExecutor);
        }
        return processExpressionExecutors;
    }

    private static void populateFinalBaseAggregators(
            Map<String, Table> tableMap, List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
            MetaStreamEvent incomingMetaStreamEvent, List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
            SiddhiQueryContext siddhiQueryContext, List<Expression> finalBaseAggregators) {

        List<Attribute> finalBaseAttributes = new ArrayList<>();

        for (IncrementalAttributeAggregator incrementalAttributeAggregator : incrementalAttributeAggregators) {
            Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
            Expression[] baseAttributeInitialValues = incrementalAttributeAggregator.getBaseAttributeInitialValues();
            Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

            for (int i = 0; i < baseAttributes.length; i++) {
                validateBaseAggregators(incrementalAttributeAggregators,
                        incrementalAttributeAggregator, baseAttributes,
                        baseAttributeInitialValues, baseAggregators, i);
                if (!finalBaseAttributes.contains(baseAttributes[i])) {
                    finalBaseAttributes.add(baseAttributes[i]);
                    finalBaseAggregators.add(baseAggregators[i]);
                    incomingMetaStreamEvent.addOutputData(baseAttributes[i]);
                    incomingExpressionExecutors.add(ExpressionParser.parseExpression(baseAttributeInitialValues[i],
                            incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                            false, 0,
                            ProcessingMode.BATCH, false, siddhiQueryContext));
                }
            }
        }
    }

    private static void populateIncomingAggregatorsAndExecutors(
            AggregationDefinition aggregationDefinition, SiddhiQueryContext siddhiQueryContext,
            Map<String, Table> tableMap, List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
            MetaStreamEvent incomingMetaStreamEvent, List<ExpressionExecutor> incomingExpressionExecutors,
            List<IncrementalAttributeAggregator> incrementalAttributeAggregators, List<Variable> groupByVariableList,
            List<Expression> outputExpressions, boolean isProcessingOnExternalTime, boolean isDistributed,
            String shardId) {

        boolean isLatestEventAdded = false;

        ExpressionExecutor timestampExecutor = getTimeStampExecutor(siddhiQueryContext, tableMap,
                incomingVariableExpressionExecutors, incomingMetaStreamEvent);

        Attribute timestampAttribute = new Attribute(AGG_START_TIMESTAMP_COL, Attribute.Type.LONG);
        incomingMetaStreamEvent.addOutputData(timestampAttribute);
        incomingExpressionExecutors.add(timestampExecutor);

        if (isDistributed) {
            ExpressionExecutor nodeIdExpExecutor = new ConstantExpressionExecutor(shardId, Attribute.Type.STRING);
            incomingExpressionExecutors.add(nodeIdExpExecutor);
            incomingMetaStreamEvent.addOutputData(new Attribute(AGG_SHARD_ID_COL, Attribute.Type.STRING));
        }

        ExpressionExecutor externalTimestampExecutor = null;
        if (isProcessingOnExternalTime) {
            Expression externalTimestampExpression = aggregationDefinition.getAggregateAttribute();
            externalTimestampExecutor = ExpressionParser.parseExpression(externalTimestampExpression,
                    incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);

            if (externalTimestampExecutor.getReturnType() == Attribute.Type.STRING) {
                Expression expression = AttributeFunction.function("incrementalAggregator",
                        "timestampInMilliseconds", externalTimestampExpression);
                externalTimestampExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                        0, tableMap, incomingVariableExpressionExecutors, false, 0, ProcessingMode.BATCH, false,
                        siddhiQueryContext);
            } else if (externalTimestampExecutor.getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppCreationException(
                        "AggregationDefinition '" + aggregationDefinition.getId() + "'s aggregateAttribute expects " +
                                "long or string, but found " + timestampExecutor.getReturnType() + ". " +
                                "Hence, can't create the siddhi app '" +
                                siddhiQueryContext.getSiddhiAppContext().getName() + "'",
                        externalTimestampExpression.getQueryContextStartIndex(),
                        externalTimestampExpression.getQueryContextEndIndex());
            }

            Attribute externalTimestampAttribute = new Attribute(AGG_EXTERNAL_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(externalTimestampAttribute);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }

        AbstractDefinition incomingLastInputStreamDefinition = incomingMetaStreamEvent.getLastInputDefinition();
        for (Variable groupByVariable : groupByVariableList) {

            incomingMetaStreamEvent.addOutputData(incomingLastInputStreamDefinition.getAttributeList()
                    .get(incomingLastInputStreamDefinition.getAttributePosition(
                            groupByVariable.getAttributeName())));
            incomingExpressionExecutors.add(ExpressionParser.parseExpression(groupByVariable,
                    incomingMetaStreamEvent, 0, tableMap, incomingVariableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH,
                    false, siddhiQueryContext));
        }

        // Add AGG_TIMESTAMP to output as well
        aggregationDefinition.getAttributeList().add(timestampAttribute);

        //Executors of time is differentiated with modes
        if (isProcessingOnExternalTime) {
            outputExpressions.add(Expression.variable(AGG_EXTERNAL_TIMESTAMP_COL));
        } else {
            outputExpressions.add(Expression.variable(AGG_START_TIMESTAMP_COL));
        }

        for (OutputAttribute outputAttribute : aggregationDefinition.getSelector().getSelectionList()) {
            Expression expression = outputAttribute.getExpression();
            if (expression instanceof AttributeFunction) {
                IncrementalAttributeAggregator incrementalAggregator = null;
                try {
                    incrementalAggregator = (IncrementalAttributeAggregator)
                            SiddhiClassLoader.loadExtensionImplementation(
                                    new AttributeFunction("incrementalAggregator",
                                            ((AttributeFunction) expression).getName(),
                                            ((AttributeFunction) expression).getParameters()),
                                    IncrementalAttributeAggregatorExtensionHolder.getInstance(
                                            siddhiQueryContext.getSiddhiAppContext()));
                } catch (SiddhiAppCreationException ex) {
                    try {
                        SiddhiClassLoader.loadExtensionImplementation((AttributeFunction) expression,
                                FunctionExecutorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
                        processAggregationSelectors(aggregationDefinition, siddhiQueryContext, tableMap,
                                incomingVariableExpressionExecutors, incomingMetaStreamEvent,
                                incomingExpressionExecutors, outputExpressions, outputAttribute, expression);

                    } catch (SiddhiAppCreationException e) {
                        throw new SiddhiAppCreationException("'" + ((AttributeFunction) expression).getName() +
                                "' is neither a incremental attribute aggregator extension or a function" +
                                " extension", expression.getQueryContextStartIndex(),
                                expression.getQueryContextEndIndex());
                    }
                }
                if (incrementalAggregator != null) {
                    initIncrementalAttributeAggregator(incomingLastInputStreamDefinition,
                            (AttributeFunction) expression, incrementalAggregator);
                    incrementalAttributeAggregators.add(incrementalAggregator);
                    aggregationDefinition.getAttributeList().add(
                            new Attribute(outputAttribute.getRename(), incrementalAggregator.getReturnType()));
                    outputExpressions.add(incrementalAggregator.aggregate());
                }
            } else {
                if (expression instanceof Variable && groupByVariableList.contains(expression)) {
                    Attribute groupByAttribute = null;
                    for (Attribute attribute : incomingMetaStreamEvent.getOutputData()) {
                        if (attribute.getName().equals(((Variable) expression).getAttributeName())) {
                            groupByAttribute = attribute;
                            break;
                        }
                    }
                    if (groupByAttribute == null) {
                        throw new SiddhiAppCreationException("Expected GroupBy attribute '" +
                                ((Variable) expression).getAttributeName() + "' not used in aggregation '" +
                                siddhiQueryContext.getName() + "' processing.", expression.getQueryContextStartIndex(),
                                expression.getQueryContextEndIndex());
                    }
                    aggregationDefinition.getAttributeList().add(
                            new Attribute(outputAttribute.getRename(), groupByAttribute.getType()));
                    outputExpressions.add(Expression.variable(groupByAttribute.getName()));

                } else {
                    isLatestEventAdded = true;
                    processAggregationSelectors(aggregationDefinition, siddhiQueryContext, tableMap,
                            incomingVariableExpressionExecutors, incomingMetaStreamEvent,
                            incomingExpressionExecutors, outputExpressions, outputAttribute, expression);
                }

            }
        }

        if (isProcessingOnExternalTime && isLatestEventAdded) {
            Attribute lastEventTimeStamp = new Attribute(AGG_LAST_TIMESTAMP_COL, Attribute.Type.LONG);
            incomingMetaStreamEvent.addOutputData(lastEventTimeStamp);
            incomingExpressionExecutors.add(externalTimestampExecutor);
        }

    }

    private static void processAggregationSelectors(AggregationDefinition aggregationDefinition,
                                                    SiddhiQueryContext siddhiQueryContext, Map<String, Table> tableMap,
                                                    List<VariableExpressionExecutor> incomingVariableExpressionExecutors,
                                                    MetaStreamEvent incomingMetaStreamEvent,
                                                    List<ExpressionExecutor> incomingExpressionExecutors,
                                                    List<Expression> outputExpressions, OutputAttribute outputAttribute,
                                                    Expression expression) {

        ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression, incomingMetaStreamEvent,
                0, tableMap, incomingVariableExpressionExecutors, false, 0, ProcessingMode.BATCH, false,
                siddhiQueryContext);
        incomingExpressionExecutors.add(expressionExecutor);
        incomingMetaStreamEvent.addOutputData(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        aggregationDefinition.getAttributeList().add(
                new Attribute(outputAttribute.getRename(), expressionExecutor.getReturnType()));
        outputExpressions.add(Expression.variable(outputAttribute.getRename()));
    }


    private static void validateBaseAggregators(List<IncrementalAttributeAggregator> incrementalAttributeAggregators,
                                                IncrementalAttributeAggregator incrementalAttributeAggregator,
                                                Attribute[] baseAttributes, Expression[] baseAttributeInitialValues,
                                                Expression[] baseAggregators, int i) {
        for (int i1 = i; i1 < incrementalAttributeAggregators.size(); i1++) {
            IncrementalAttributeAggregator otherAttributeAggregator = incrementalAttributeAggregators.get(i1);
            if (otherAttributeAggregator != incrementalAttributeAggregator) {
                Attribute[] otherBaseAttributes = otherAttributeAggregator.getBaseAttributes();
                Expression[] otherBaseAttributeInitialValues = otherAttributeAggregator
                        .getBaseAttributeInitialValues();
                Expression[] otherBaseAggregators = otherAttributeAggregator.getBaseAggregators();
                for (int j = 0; j < otherBaseAttributes.length; j++) {
                    if (baseAttributes[i].equals(otherBaseAttributes[j])) {
                        if (!baseAttributeInitialValues[i].equals(otherBaseAttributeInitialValues[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same initial values, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different initial values.");
                        }
                        if (!baseAggregators[i].equals(otherBaseAggregators[j])) {
                            throw new SiddhiAppCreationException("BaseAttributes having same name should " +
                                    "be defined with same baseAggregators, but baseAttribute '" +
                                    baseAttributes[i] + "' is defined in '" +
                                    incrementalAttributeAggregator.getClass().getName() + "' and '" +
                                    otherAttributeAggregator.getClass().getName() +
                                    "' with different baseAggregators.");
                        }
                    }
                }
            }
        }
    }

    private static void initIncrementalAttributeAggregator(AbstractDefinition lastInputStreamDefinition,
                                                           AttributeFunction attributeFunction,
                                                           IncrementalAttributeAggregator incrementalAttributeAggregator) {

        String attributeName = null;
        Attribute.Type attributeType = null;
        if (attributeFunction.getParameters() != null && attributeFunction.getParameters()[0] != null) {
            if (attributeFunction.getParameters().length != 1) {
                throw new SiddhiAppCreationException("Incremental aggregator requires only one parameter. "
                        + "Found " + attributeFunction.getParameters().length,
                        attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
            }
            if (!(attributeFunction.getParameters()[0] instanceof Variable)) {
                throw new SiddhiAppCreationException("Incremental aggregator expected a variable. " +
                        "However a parameter of type " + attributeFunction.getParameters()[0].getClass().getTypeName()
                        + " was found",
                        attributeFunction.getParameters()[0].getQueryContextStartIndex(),
                        attributeFunction.getParameters()[0].getQueryContextEndIndex());
            }
            attributeName = ((Variable) attributeFunction.getParameters()[0]).getAttributeName();
            attributeType = lastInputStreamDefinition.getAttributeType(attributeName);
        }

        incrementalAttributeAggregator.init(attributeName, attributeType);

        Attribute[] baseAttributes = incrementalAttributeAggregator.getBaseAttributes();
        Expression[] baseAttributeInitialValues = incrementalAttributeAggregator
                .getBaseAttributeInitialValues();
        Expression[] baseAggregators = incrementalAttributeAggregator.getBaseAggregators();

        if (baseAttributes.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributes '" +
                    baseAttributes.length + "' is not equal for '" + attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
        if (baseAttributeInitialValues.length != baseAggregators.length) {
            throw new SiddhiAppCreationException("Number of baseAggregators '" +
                    baseAggregators.length + "' and baseAttributeInitialValues '" +
                    baseAttributeInitialValues.length + "' is not equal for '" +
                    attributeFunction + "'",
                    attributeFunction.getQueryContextStartIndex(), attributeFunction.getQueryContextEndIndex());
        }
    }

    private static ExpressionExecutor getTimeStampExecutor(SiddhiQueryContext siddhiQueryContext,
                                                           Map<String, Table> tableMap,
                                                           List<VariableExpressionExecutor> variableExpressionExecutors,
                                                           MetaStreamEvent metaStreamEvent) {

        Expression timestampExpression;
        ExpressionExecutor timestampExecutor;

        // Execution is based on system time, the GMT time zone would be used.
        timestampExpression = AttributeFunction.function("currentTimeMillis", null);
        timestampExecutor = ExpressionParser.parseExpression(timestampExpression, metaStreamEvent, 0, tableMap,
                variableExpressionExecutors, false, 0, ProcessingMode.BATCH, false, siddhiQueryContext);
        return timestampExecutor;
    }

    private static boolean isRange(TimePeriod timePeriod) {
        return timePeriod.getOperator() == TimePeriod.Operator.RANGE;
    }

    private static List<TimePeriod.Duration> getSortedPeriods(TimePeriod timePeriod) {
        try {
            List<TimePeriod.Duration> durations = timePeriod.getDurations();
            if (isRange(timePeriod)) {
                durations = fillGap(durations.get(0), durations.get(1));
            }
            return sortedDurations(durations);
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, timePeriod, null);
            throw t;
        }
    }

    private static List<TimePeriod.Duration> sortedDurations(List<TimePeriod.Duration> durations) {
        List<TimePeriod.Duration> copyDurations = new ArrayList<>(durations);

        Comparator periodComparator = (Comparator<TimePeriod.Duration>) (firstDuration, secondDuration) -> {
            int firstOrdinal = firstDuration.ordinal();
            int secondOrdinal = secondDuration.ordinal();
            if (firstOrdinal > secondOrdinal) {
                return 1;
            } else if (firstOrdinal < secondOrdinal) {
                return -1;
            }
            return 0;
        };
        copyDurations.sort(periodComparator);
        return copyDurations;
    }

    private static List<TimePeriod.Duration> fillGap(TimePeriod.Duration start, TimePeriod.Duration end) {
        TimePeriod.Duration[] durations = TimePeriod.Duration.values();
        List<TimePeriod.Duration> filledDurations = new ArrayList<>();

        int startIndex = start.ordinal();
        int endIndex = end.ordinal();

        if (startIndex > endIndex) {
            throw new SiddhiAppCreationException(
                    "Start time period must be less than end time period for range aggregation calculation");
        }

        if (startIndex == endIndex) {
            filledDurations.add(start);
        } else {
            TimePeriod.Duration[] temp = new TimePeriod.Duration[endIndex - startIndex + 1];
            System.arraycopy(durations, startIndex, temp, 0, endIndex - startIndex + 1);
            filledDurations = Arrays.asList(temp);
        }
        return filledDurations;
    }

    private static HashMap<TimePeriod.Duration, Table> initDefaultTables(
            String aggregatorName, List<TimePeriod.Duration> durations,
            StreamDefinition streamDefinition, SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
            List<Annotation> annotations, List<Variable> groupByVariableList, boolean isProcessingOnExternalTime,
            boolean enablePartioning) {

        HashMap<TimePeriod.Duration, Table> aggregationTableMap = new HashMap<>();

        // Create annotations for primary key
        Annotation primaryKeyAnnotation = new Annotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY);
        primaryKeyAnnotation.element(null, AGG_START_TIMESTAMP_COL);

        if (enablePartioning) {
            primaryKeyAnnotation.element(null, AGG_SHARD_ID_COL);
        }
        if (isProcessingOnExternalTime) {
            primaryKeyAnnotation.element(null, AGG_EXTERNAL_TIMESTAMP_COL);
        }
        for (Variable groupByVariable : groupByVariableList) {
            primaryKeyAnnotation.element(null, groupByVariable.getAttributeName());
        }
        annotations.add(primaryKeyAnnotation);
        for (TimePeriod.Duration duration : durations) {
            String tableId = aggregatorName + "_" + duration.toString();
            TableDefinition tableDefinition = TableDefinition.id(tableId);
            for (Attribute attribute : streamDefinition.getAttributeList()) {
                tableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            annotations.forEach(tableDefinition::annotation);
            siddhiAppRuntimeBuilder.defineTable(tableDefinition);
            aggregationTableMap.put(duration, siddhiAppRuntimeBuilder.getTableMap().get(tableId));
        }
        return aggregationTableMap;
    }

    public static StreamEvent createRestEvent(MetaStreamEvent metaStreamEvent, StreamEvent streamEvent) {
        streamEvent.setTimestamp(0);
        streamEvent.setType(ComplexEvent.Type.RESET);
        List<Attribute> outputData = metaStreamEvent.getOutputData();
        for (int i = 0, outputDataSize = outputData.size(); i < outputDataSize; i++) {
            Attribute attribute = outputData.get(i);
            switch (attribute.getType()) {

                case STRING:
                    streamEvent.setOutputData("", i);
                    break;
                case INT:
                    streamEvent.setOutputData(0, i);
                    break;
                case LONG:
                    streamEvent.setOutputData(0L, i);
                    break;
                case FLOAT:
                    streamEvent.setOutputData(0f, i);
                    break;
                case DOUBLE:
                    streamEvent.setOutputData(0.0, i);
                    break;
                case BOOL:
                    streamEvent.setOutputData(false, i);
                    break;
                case OBJECT:
                    streamEvent.setOutputData(null, i);
                    break;
            }
        }
        return streamEvent;
    }
}

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

package io.siddhi.core.util.collection.operator;

import io.siddhi.core.aggregation.Executor;
import io.siddhi.core.aggregation.IncrementalDataAggregator;
import io.siddhi.core.aggregation.IncrementalExecutor;
import io.siddhi.core.aggregation.OutOfOrderEventsDataAggregator;
import io.siddhi.core.aggregation.persistedaggregation.PersistedIncrementalExecutor;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.siddhi.query.api.expression.Expression.Time.normalizeDuration;

/**
 * Defines the logic to find a matching event from an incremental aggregator (retrieval from incremental aggregator),
 * based on the logical conditions defined herewith.
 */
public class IncrementalAggregateCompileCondition implements CompiledCondition {
    private static final Logger LOG = LogManager.getLogger(IncrementalAggregateCompileCondition.class);

    private final boolean isOnDemandQuery;

    private final String aggregationName;
    private final boolean isProcessingOnExternalTime;
    private final boolean isDistributed;
    private final List<TimePeriod.Duration> activeIncrementalDurations;
    private final StreamEventFactory streamEventFactoryForTableMeta;
    private final StreamEventCloner tableEventCloner;
    private final StreamEventFactory streamEventFactoryForAggregateMeta;
    private final StreamEventCloner aggregateEventCloner;
    private Map<TimePeriod.Duration, Table> aggregationTableMap;
    private List<ExpressionExecutor> outputExpressionExecutors;
    private boolean isOptimisedLookup;
    private Map<TimePeriod.Duration, CompiledSelection> withinTableCompiledSelection;
    private Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions;
    private CompiledCondition inMemoryStoreCompileCondition;
    private Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition;
    private CompiledCondition onCompiledCondition;
    private List<Attribute> additionalAttributes;
    private ExpressionExecutor perExpressionExecutor;
    private ExpressionExecutor startTimeEndTimeExpressionExecutor;
    private List<ExpressionExecutor> timestampFilterExecutors;
    private MetaStreamEvent tableMetaStreamEvent;
    private ComplexEventPopulater complexEventPopulater;
    private MatchingMetaInfoHolder alteredMatchingMetaInfoHolder;


    private MatchingMetaInfoHolder matchingHolderInfoForTableLookups;
    private List<VariableExpressionExecutor> variableExpExecutorsForTableLookups;

    public IncrementalAggregateCompileCondition(
            boolean isOnDemandQuery,
            String aggregationName, boolean isProcessingOnExternalTime, boolean isDistributed,
            List<TimePeriod.Duration> activeIncrementalDurations, Map<TimePeriod.Duration, Table> aggregationTableMap,
            List<ExpressionExecutor> outputExpressionExecutors, boolean isOptimisedLookup,
            Map<TimePeriod.Duration, CompiledSelection> withinTableCompiledSelection,
            Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions,
            CompiledCondition inMemoryStoreCompileCondition,
            Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition,
            CompiledCondition onCompiledCondition, List<Attribute> additionalAttributes,
            ExpressionExecutor perExpressionExecutor, ExpressionExecutor startTimeEndTimeExpressionExecutor,
            List<ExpressionExecutor> timestampFilterExecutors,
            MetaStreamEvent aggregateMetaSteamEvent, MatchingMetaInfoHolder alteredMatchingMetaInfoHolder,
            MatchingMetaInfoHolder matchingHolderInfoForTableLookups,
            List<VariableExpressionExecutor> variableExpExecutorsForTableLookups) {

        this.isOnDemandQuery = isOnDemandQuery;
        this.aggregationName = aggregationName;
        this.isProcessingOnExternalTime = isProcessingOnExternalTime;
        this.isDistributed = isDistributed;
        this.activeIncrementalDurations = activeIncrementalDurations;
        this.aggregationTableMap = aggregationTableMap;
        this.outputExpressionExecutors = outputExpressionExecutors;

        this.isOptimisedLookup = isOptimisedLookup;
        this.withinTableCompiledSelection = withinTableCompiledSelection;
        this.withinTableCompiledConditions = withinTableCompiledConditions;
        this.inMemoryStoreCompileCondition = inMemoryStoreCompileCondition;
        this.withinTableLowerGranularityCompileCondition = withinTableLowerGranularityCompileCondition;
        this.onCompiledCondition = onCompiledCondition;

        this.additionalAttributes = additionalAttributes;
        this.perExpressionExecutor = perExpressionExecutor;
        this.startTimeEndTimeExpressionExecutor = startTimeEndTimeExpressionExecutor;
        this.timestampFilterExecutors = timestampFilterExecutors;

        this.tableMetaStreamEvent = matchingHolderInfoForTableLookups.getMetaStateEvent().getMetaStreamEvent(1);
        this.streamEventFactoryForTableMeta = new StreamEventFactory(tableMetaStreamEvent);
        this.tableEventCloner = new StreamEventCloner(tableMetaStreamEvent, streamEventFactoryForTableMeta);
        this.streamEventFactoryForAggregateMeta = new StreamEventFactory(aggregateMetaSteamEvent);
        this.aggregateEventCloner = new StreamEventCloner(aggregateMetaSteamEvent, streamEventFactoryForAggregateMeta);

        this.alteredMatchingMetaInfoHolder = alteredMatchingMetaInfoHolder;
        this.matchingHolderInfoForTableLookups = matchingHolderInfoForTableLookups;
        this.variableExpExecutorsForTableLookups = variableExpExecutorsForTableLookups;

    }

    public void init() {
        QueryParserHelper.updateVariablePosition(matchingHolderInfoForTableLookups.getMetaStateEvent(),
                variableExpExecutorsForTableLookups);
    }

    public StreamEvent find(StateEvent matchingEvent,
                            Map<TimePeriod.Duration, Executor> incrementalExecutorMap,
                            Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap,
                            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorMap,
                            ExpressionExecutor shouldUpdateTimestamp, String timeZone) {

        ComplexEventChunk<StreamEvent> complexEventChunkToHoldWithinMatches = new ComplexEventChunk<>();

        //Create matching event if it is on-demand query
        int additionTimestampAttributesSize = this.timestampFilterExecutors.size() + 2;
        Long[] timestampFilters = new Long[additionTimestampAttributesSize];
        if (matchingEvent.getStreamEvent(0) == null) {
            StreamEvent streamEvent = new StreamEvent(0, additionTimestampAttributesSize, 0);
            matchingEvent.addEvent(0, streamEvent);
        }

        Long[] startTimeEndTime = (Long[]) startTimeEndTimeExpressionExecutor.execute(matchingEvent);
        if (startTimeEndTime == null) {
            throw new SiddhiAppRuntimeException("Start and end times for within duration cannot be retrieved");
        }
        timestampFilters[0] = startTimeEndTime[0];
        timestampFilters[1] = startTimeEndTime[1];

        if (isDistributed) {
            for (int i = 0; i < additionTimestampAttributesSize - 2; i++) {
                timestampFilters[i + 2] = ((Long) this.timestampFilterExecutors.get(i).execute(matchingEvent));
            }
        }


        complexEventPopulater.populateComplexEvent(matchingEvent.getStreamEvent(0), timestampFilters);

        // Get all the aggregates within the given duration, from table corresponding to "per" duration
        // Retrieve per value
        String perValueAsString = perExpressionExecutor.execute(matchingEvent).toString();
        TimePeriod.Duration perValue;
        try {
            // Per time function verification
            perValue = normalizeDuration(perValueAsString);
        } catch (SiddhiAppValidationException e) {
            throw new SiddhiAppRuntimeException(
                    "Aggregation Query's per value is expected to be of a valid time function of the " +
                            "following " + TimePeriod.Duration.SECONDS + ", " + TimePeriod.Duration.MINUTES + ", "
                            + TimePeriod.Duration.HOURS + ", " + TimePeriod.Duration.DAYS + ", "
                            + TimePeriod.Duration.MONTHS + ", " + TimePeriod.Duration.YEARS + ".");
        }
        if (!incrementalExecutorMap.keySet().contains(perValue)) {
            throw new SiddhiAppRuntimeException("The aggregate values for " + perValue.toString()
                    + " granularity cannot be provided since aggregation definition " + aggregationName
                    + " does not contain " + perValue.toString() + " duration");
        }

        Table tableForPerDuration = aggregationTableMap.get(perValue);

        StreamEvent withinMatchFromPersistedEvents;
        if (isOptimisedLookup) {
            withinMatchFromPersistedEvents = query(tableForPerDuration, matchingEvent,
                    withinTableCompiledConditions.get(perValue), withinTableCompiledSelection.get(perValue),
                    tableMetaStreamEvent.getLastInputDefinition().getAttributeList().toArray(new Attribute[0]));
        } else {
            withinMatchFromPersistedEvents = tableForPerDuration.find(matchingEvent,
                    withinTableCompiledConditions.get(perValue));
        }
        complexEventChunkToHoldWithinMatches.add(withinMatchFromPersistedEvents);

        // Optimization step.
        long oldestInMemoryEventTimestamp = getOldestInMemoryEventTimestamp(incrementalExecutorMap,
                activeIncrementalDurations, perValue);

        //If processing on external time, the in-memory data also needs to be queried
        if (isProcessingOnExternalTime || requiresAggregatingInMemoryData(oldestInMemoryEventTimestamp,
                startTimeEndTime)) {
            if (isDistributed) {
                int perValueIndex = this.activeIncrementalDurations.indexOf(perValue);
                if (perValueIndex != 0) {
                    Map<TimePeriod.Duration, CompiledCondition> lowerGranularityLookups = new HashMap<>();
                    for (int i = 0; i < perValueIndex; i++) {
                        TimePeriod.Duration key = this.activeIncrementalDurations.get(i);
                        lowerGranularityLookups.put(key, withinTableLowerGranularityCompileCondition.get(key));
                    }
                    List<StreamEvent> eventChunks = lowerGranularityLookups.entrySet().stream()
                            .map((entry) -> {
                                Table table = aggregationTableMap.get(entry.getKey());
                                if (isOptimisedLookup) {
                                    return query(table, matchingEvent, entry.getValue(),
                                            withinTableCompiledSelection.get(entry.getKey()),
                                            tableMetaStreamEvent.getLastInputDefinition()
                                                    .getAttributeList().toArray(new Attribute[0]));
                                } else {
                                    return table.find(matchingEvent, entry.getValue());
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    eventChunks.forEach(complexEventChunkToHoldWithinMatches::add);
                }
            } else {
                TimePeriod.Duration rootDuration = activeIncrementalDurations.get(0);

                IncrementalDataAggregator incrementalDataAggregator = new IncrementalDataAggregator(
                        activeIncrementalDurations, perValue, oldestInMemoryEventTimestamp,
                        aggregateProcessingExecutorsMap.get(rootDuration), shouldUpdateTimestamp,
                        groupByKeyGeneratorMap.get(rootDuration) != null, tableMetaStreamEvent, timeZone);
                ComplexEventChunk<StreamEvent> aggregatedInMemoryEventChunk;
                // Aggregate in-memory data and create an event chunk out of it
                aggregatedInMemoryEventChunk = incrementalDataAggregator.aggregateInMemoryData(incrementalExecutorMap);

                // Get the in-memory aggregate data, which is within given duration
                StreamEvent withinMatchFromInMemory = ((Operator) inMemoryStoreCompileCondition).find(matchingEvent,
                        aggregatedInMemoryEventChunk, tableEventCloner);
                complexEventChunkToHoldWithinMatches.add(withinMatchFromInMemory);
            }
        }

        ComplexEventChunk<StreamEvent> processedEvents;
        if (isDistributed || isProcessingOnExternalTime) {

            List<ExpressionExecutor> expressionExecutors = aggregateProcessingExecutorsMap.get(perValue);
            GroupByKeyGenerator groupByKeyGenerator = groupByKeyGeneratorMap.get(perValue);
            OutOfOrderEventsDataAggregator outOfOrderEventsDataAggregator =
                    new OutOfOrderEventsDataAggregator(expressionExecutors, shouldUpdateTimestamp, groupByKeyGenerator,
                            tableMetaStreamEvent);
            processedEvents = outOfOrderEventsDataAggregator
                    .aggregateData(complexEventChunkToHoldWithinMatches);
        } else {
            processedEvents = complexEventChunkToHoldWithinMatches;
        }

        // Get the final event chunk from the data which is within given duration. This event chunk contains the values
        // in the select clause of an aggregate definition.
        ComplexEventChunk<StreamEvent> aggregateSelectionComplexEventChunk = createAggregateSelectionEventChunk(
                processedEvents, outputExpressionExecutors);

        // Execute the on compile condition
        return ((Operator) onCompiledCondition).find(matchingEvent, aggregateSelectionComplexEventChunk,
                aggregateEventCloner);
    }

    private StreamEvent query(Table tableForPerDuration, StateEvent matchingEvent, CompiledCondition compiledCondition,
                              CompiledSelection compiledSelection, Attribute[] outputAttributes) {
        if (tableForPerDuration.getIsConnected()) {
            try {
                return ((QueryableProcessor) tableForPerDuration)
                        .query(matchingEvent, compiledCondition, compiledSelection, outputAttributes);
            } catch (ConnectionUnavailableException e) {
                // On-demand query does not have retry logic and retry called manually
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unable to query table '" + tableForPerDuration.getTableDefinition().getId() + "', "
                            + "as the datasource is unavailable.");
                }
                if (!isOnDemandQuery) {
                    tableForPerDuration.setIsConnectedToFalse();
                    tableForPerDuration.connectWithRetry();
                    return query(tableForPerDuration, matchingEvent, compiledCondition, compiledSelection,
                            outputAttributes);
                }
                throw new SiddhiAppRuntimeException(e.getMessage(), e);
            }
        } else if (tableForPerDuration.getIsTryingToConnect()) {
            LOG.warn("Error on '" + aggregationName + "' while performing query for event '" + matchingEvent +
                    "', operation busy waiting at Table '" + tableForPerDuration.getTableDefinition().getId() +
                    "' as its trying to reconnect!");
            tableForPerDuration.waitWhileConnect();
            LOG.info("Aggregation '" + aggregationName + "' table '" +
                    tableForPerDuration.getTableDefinition().getId() + "' has become available for query for " +
                    "matching event '" + matchingEvent + "'");
            return query(tableForPerDuration, matchingEvent, compiledCondition, compiledSelection,
                    outputAttributes);
        } else {
            tableForPerDuration.connectWithRetry();
            return query(tableForPerDuration, matchingEvent, compiledCondition, compiledSelection,
                    outputAttributes);

        }
    }

    private ComplexEventChunk<StreamEvent> createAggregateSelectionEventChunk(
            ComplexEventChunk<StreamEvent> complexEventChunkToHoldMatches,
            List<ExpressionExecutor> outputExpressionExecutors) {
        ComplexEventChunk<StreamEvent> aggregateSelectionComplexEventChunk = new ComplexEventChunk<>();
        StreamEvent resetEvent = streamEventFactoryForTableMeta.newInstance();
        resetEvent.setType(ComplexEvent.Type.RESET);

        while (complexEventChunkToHoldMatches.hasNext()) {
            StreamEvent streamEvent = complexEventChunkToHoldMatches.next();
            StreamEvent newStreamEvent = streamEventFactoryForAggregateMeta.newInstance();
            Object[] outputData = new Object[newStreamEvent.getOutputData().length];
            for (int i = 0; i < outputExpressionExecutors.size(); i++) {
                outputData[i] = outputExpressionExecutors.get(i).execute(streamEvent);
            }
            newStreamEvent.setTimestamp(streamEvent.getTimestamp());
            newStreamEvent.setOutputData(outputData);
            aggregateSelectionComplexEventChunk.add(newStreamEvent);
        }

        for (ExpressionExecutor expressionExecutor : outputExpressionExecutors) {
            expressionExecutor.execute(resetEvent);
        }

        return aggregateSelectionComplexEventChunk;
    }

    private boolean requiresAggregatingInMemoryData(long oldestInMemoryEventTimestamp, Long[] startTimeEndTime) {
        if (oldestInMemoryEventTimestamp == -1) {
            return false;
        }
        long endTimeForWithin = startTimeEndTime[1];
        return endTimeForWithin > oldestInMemoryEventTimestamp;
    }

    private long getOldestInMemoryEventTimestamp(Map<TimePeriod.Duration, Executor> incrementalExecutorMap,
                                                 List<TimePeriod.Duration> incrementalDurations,
                                                 TimePeriod.Duration perValue) {
        long oldestEvent;
        TimePeriod.Duration incrementalDuration;
        for (int i = perValue.ordinal(); i >= incrementalDurations.get(0).ordinal(); i--) {
            incrementalDuration = TimePeriod.Duration.values()[i];
            //If the reduced granularity is not configured
            if (incrementalExecutorMap.containsKey(incrementalDuration)) {
                if (incrementalExecutorMap.get(incrementalDuration) instanceof PersistedIncrementalExecutor) {
                    return -1;
                }
                oldestEvent = ((IncrementalExecutor) incrementalExecutorMap.get(incrementalDuration)).
                        getAggregationStartTimestamp();
                if (oldestEvent != -1) {
                    return oldestEvent;
                }
            }
        }
        return -1;
    }

    public void setComplexEventPopulater(ComplexEventPopulater complexEventPopulater) {
        this.complexEventPopulater = complexEventPopulater;
    }

    public List<Attribute> getAdditionalAttributes() {
        return this.additionalAttributes;
    }

    public MatchingMetaInfoHolder getAlteredMatchingMetaInfoHolder() {
        return this.alteredMatchingMetaInfoHolder;
    }
}

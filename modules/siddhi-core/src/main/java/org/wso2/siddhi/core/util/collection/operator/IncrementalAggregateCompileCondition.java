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

package org.wso2.siddhi.core.util.collection.operator;

import org.wso2.siddhi.core.aggregation.IncrementalDataAggregator;
import org.wso2.siddhi.core.aggregation.IncrementalExecutor;
import org.wso2.siddhi.core.aggregation.OutOfOrderEventsDataAggregator;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.selector.GroupByKeyGenerator;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.wso2.siddhi.core.util.ExpressionExecutorClonerUtil.getExpressionExecutorClone;
import static org.wso2.siddhi.core.util.ExpressionExecutorClonerUtil.getExpressionExecutorClones;
import static org.wso2.siddhi.query.api.expression.Expression.Time.normalizeDuration;

/**
 * Defines the logic to find a matching event from an incremental aggregator (retrieval from incremental aggregator),
 * based on the logical conditions defined herewith.
 */
public class IncrementalAggregateCompileCondition implements CompiledCondition {

    private final String aggregationName;
    private final boolean isProcessingOnExternalTime;
    private final boolean isDistributed;
    private final List<TimePeriod.Duration> incrementalDurations;
    private Map<TimePeriod.Duration, Table> aggregationTableMap;
    private List<ExpressionExecutor> outputExpressionExecutors;

    private Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions;
    private CompiledCondition inMemoryStoreCompileCondition;
    private Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition;
    private CompiledCondition onCompiledCondition;
    private List<Attribute> additionalAttributes;
    private ExpressionExecutor perExpressionExecutor;
    private ExpressionExecutor startTimeEndTimeExpressionExecutor;
    private List<ExpressionExecutor> timestampFilterExecutors;

    private MetaStreamEvent tableMetaStreamEvent;
    private MetaStreamEvent aggregateMetaStreamEvent;
    private ComplexEventPopulater complexEventPopulater;
    private MatchingMetaInfoHolder alteredMatchingMetaInfoHolder;

    private final StreamEventPool streamEventPoolForTableMeta;
    private final StreamEventCloner tableEventCloner;
    private final StreamEventPool streamEventPoolForAggregateMeta;
    private final StreamEventCloner aggregateEventCloner;

    private MatchingMetaInfoHolder matchingHolderInfoForTableLookups;
    private List<VariableExpressionExecutor> variableExpExecutorsForTableLookups;

    public IncrementalAggregateCompileCondition(
            String aggregationName, boolean isProcessingOnExternalTime, boolean isDistributed,
            List<TimePeriod.Duration> incrementalDurations, Map<TimePeriod.Duration, Table> aggregationTableMap,
            List<ExpressionExecutor> outputExpressionExecutors,
            Map<TimePeriod.Duration, CompiledCondition> withinTableCompiledConditions,
            CompiledCondition inMemoryStoreCompileCondition,
            Map<TimePeriod.Duration, CompiledCondition> withinTableLowerGranularityCompileCondition,
            CompiledCondition onCompiledCondition, List<Attribute> additionalAttributes,
            ExpressionExecutor perExpressionExecutor, ExpressionExecutor startTimeEndTimeExpressionExecutor,
            List<ExpressionExecutor> timestampFilterExecutors,
            MetaStreamEvent aggregateMetaSteamEvent, MatchingMetaInfoHolder alteredMatchingMetaInfoHolder,
        MatchingMetaInfoHolder matchingHolderInfoForTableLookups,
        List<VariableExpressionExecutor> variableExpExecutorsForTableLookups) {
        this.aggregationName = aggregationName;
        this.isProcessingOnExternalTime = isProcessingOnExternalTime;
        this.isDistributed = isDistributed;
        this.incrementalDurations = incrementalDurations;
        this.aggregationTableMap = aggregationTableMap;
        this.outputExpressionExecutors = outputExpressionExecutors;

        this.withinTableCompiledConditions = withinTableCompiledConditions;
        this.inMemoryStoreCompileCondition = inMemoryStoreCompileCondition;
        this.withinTableLowerGranularityCompileCondition = withinTableLowerGranularityCompileCondition;
        this.onCompiledCondition = onCompiledCondition;

        this.additionalAttributes = additionalAttributes;
        this.perExpressionExecutor = perExpressionExecutor;
        this.startTimeEndTimeExpressionExecutor = startTimeEndTimeExpressionExecutor;
        this.timestampFilterExecutors = timestampFilterExecutors;

        this.tableMetaStreamEvent = matchingHolderInfoForTableLookups.getMetaStateEvent().getMetaStreamEvent(1);
        this.aggregateMetaStreamEvent = aggregateMetaSteamEvent;
        this.streamEventPoolForTableMeta = new StreamEventPool(tableMetaStreamEvent, 10);
        this.tableEventCloner = new StreamEventCloner(tableMetaStreamEvent, streamEventPoolForTableMeta);
        this.streamEventPoolForAggregateMeta = new StreamEventPool(aggregateMetaStreamEvent, 10);
        this.aggregateEventCloner = new StreamEventCloner(aggregateMetaStreamEvent, streamEventPoolForAggregateMeta);
        this.alteredMatchingMetaInfoHolder = alteredMatchingMetaInfoHolder;
        this.matchingHolderInfoForTableLookups = matchingHolderInfoForTableLookups;
        this.variableExpExecutorsForTableLookups = variableExpExecutorsForTableLookups;
    }

    public void init() {
        QueryParserHelper.updateVariablePosition(matchingHolderInfoForTableLookups.getMetaStateEvent(),
                variableExpExecutorsForTableLookups);
    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        Map<TimePeriod.Duration, CompiledCondition> copyOfWithinTableCompiledConditions = new HashMap<>();
        for (Map.Entry<TimePeriod.Duration, CompiledCondition> entry : withinTableCompiledConditions.entrySet()) {
            copyOfWithinTableCompiledConditions.put(entry.getKey(), entry.getValue().cloneCompilation(key));
        }
        Map<TimePeriod.Duration, CompiledCondition> copyOfWithinTableLowerGranularityCompileCondition = new HashMap<>();
        for (Map.Entry<TimePeriod.Duration, CompiledCondition> entry :
                withinTableLowerGranularityCompileCondition.entrySet()) {
            copyOfWithinTableLowerGranularityCompileCondition
                    .put(entry.getKey(), entry.getValue().cloneCompilation(key));
        }
        return new IncrementalAggregateCompileCondition(aggregationName, isProcessingOnExternalTime, isDistributed,
                incrementalDurations, aggregationTableMap, getExpressionExecutorClones(outputExpressionExecutors),
                copyOfWithinTableCompiledConditions, inMemoryStoreCompileCondition.cloneCompilation(key),
                copyOfWithinTableLowerGranularityCompileCondition, onCompiledCondition.cloneCompilation(key),
                additionalAttributes, perExpressionExecutor, startTimeEndTimeExpressionExecutor,
                timestampFilterExecutors, aggregateMetaStreamEvent,
                alteredMatchingMetaInfoHolder, matchingHolderInfoForTableLookups, variableExpExecutorsForTableLookups);
    }

    public StreamEvent find(StateEvent matchingEvent,
                            Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap,
                            Map<TimePeriod.Duration, List<ExpressionExecutor>> aggregateProcessingExecutorsMap,
                            Map<TimePeriod.Duration, GroupByKeyGenerator> groupByKeyGeneratorList,
                            ExpressionExecutor shouldUpdateTimestamp) {

        ComplexEventChunk<StreamEvent> complexEventChunkToHoldWithinMatches = new ComplexEventChunk<>(true);
        //Create matching event if it is store Query
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
                    + " granularity cannot be provided since aggregation definition " +
                    aggregationName + " does not contain " + perValue.toString() + " duration");
        }

        Table tableForPerDuration = aggregationTableMap.get(perValue);

        StreamEvent withinMatchFromPersistedEvents = tableForPerDuration.find(matchingEvent,
                withinTableCompiledConditions.get(perValue));
        complexEventChunkToHoldWithinMatches.add(withinMatchFromPersistedEvents);

        // Optimization step.
        long oldestInMemoryEventTimestamp = getOldestInMemoryEventTimestamp(incrementalExecutorMap,
                incrementalDurations, perValue);

        //If processing on external time, the in-memory data also needs to be queried
        if (isProcessingOnExternalTime || requiresAggregatingInMemoryData(oldestInMemoryEventTimestamp,
                startTimeEndTime)) {
            if (isDistributed) {
                int perValueIndex = this.incrementalDurations.indexOf(perValue);
                if (perValueIndex != 0) {
                    Map<TimePeriod.Duration, CompiledCondition> lowerGranularityLookups = new HashMap<>();
                    for (int i = 0; i < perValueIndex; i++) {
                        TimePeriod.Duration key = this.incrementalDurations.get(i);
                        lowerGranularityLookups.put(key, withinTableLowerGranularityCompileCondition.get(key));
                    }
                    List<StreamEvent> eventChunks = lowerGranularityLookups.entrySet().stream()
                            .map((entry) ->
                                    aggregationTableMap.get(entry.getKey()).find(matchingEvent, entry.getValue()))
                            .collect(Collectors.toList());
                    eventChunks.forEach((eventChunk) -> {
                        if (eventChunk != null) {
                            complexEventChunkToHoldWithinMatches.add(eventChunk);
                        }
                    });
                }
            } else if (isProcessingOnExternalTime || requiresAggregatingInMemoryData(oldestInMemoryEventTimestamp,
                    startTimeEndTime)) {
                IncrementalDataAggregator incrementalDataAggregator = new IncrementalDataAggregator(
                        incrementalDurations, perValue, oldestInMemoryEventTimestamp,
                        getExpressionExecutorClones(aggregateProcessingExecutorsMap.get(incrementalDurations.get(0))),
                        getExpressionExecutorClone(shouldUpdateTimestamp),
                        tableMetaStreamEvent
                        );
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

            OutOfOrderEventsDataAggregator outOfOrderEventsDataAggregator = new OutOfOrderEventsDataAggregator(
                            getExpressionExecutorClones(aggregateProcessingExecutorsMap.get(perValue)),
                            getExpressionExecutorClone(shouldUpdateTimestamp),
                            groupByKeyGeneratorList.get(perValue), tableMetaStreamEvent);
            processedEvents = outOfOrderEventsDataAggregator.aggregateData(complexEventChunkToHoldWithinMatches);
        } else {
            processedEvents = complexEventChunkToHoldWithinMatches;
        }

        // Get the final event chunk from the data which is within given duration. This event chunk contains the values
        // in the select clause of an aggregate definition.
        ComplexEventChunk<StreamEvent> aggregateSelectionComplexEventChunk = createAggregateSelectionEventChunk(
                processedEvents, getExpressionExecutorClones(outputExpressionExecutors));

        // Execute the on compile condition
        return ((Operator) onCompiledCondition).find(matchingEvent, aggregateSelectionComplexEventChunk,
                aggregateEventCloner);
    }

    private ComplexEventChunk<StreamEvent> createAggregateSelectionEventChunk(
            ComplexEventChunk<StreamEvent> complexEventChunkToHoldMatches,
            List<ExpressionExecutor> outputExpressionExecutors) {
        ComplexEventChunk<StreamEvent> aggregateSelectionComplexEventChunk = new ComplexEventChunk<>(true);
        StreamEvent resetEvent = streamEventPoolForTableMeta.borrowEvent();
        resetEvent.setType(ComplexEvent.Type.RESET);

        while (complexEventChunkToHoldMatches.hasNext()) {
            StreamEvent streamEvent = complexEventChunkToHoldMatches.next();
            StreamEvent newStreamEvent = streamEventPoolForAggregateMeta.borrowEvent();
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

    private long getOldestInMemoryEventTimestamp(Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap,
                                                 List<TimePeriod.Duration> incrementalDurations,
                                                 TimePeriod.Duration perValue) {
        long oldestEvent;
        TimePeriod.Duration incrementalDuration;
        for (int i = perValue.ordinal(); i >= incrementalDurations.get(0).ordinal(); i--) {
            incrementalDuration = TimePeriod.Duration.values()[i];
            //If the reduced granularity is not configured
            if (incrementalExecutorMap.containsKey(incrementalDuration)) {
                oldestEvent = incrementalExecutorMap.get(incrementalDuration).getAggregationStartTimestamp();
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

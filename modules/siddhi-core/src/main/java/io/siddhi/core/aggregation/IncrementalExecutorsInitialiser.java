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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.query.OnDemandQueryRuntime;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.parser.OnDemandQueryParser;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.List;
import java.util.Map;

import static io.siddhi.core.util.SiddhiConstants.AGG_SHARD_ID_COL;
import static io.siddhi.core.util.SiddhiConstants.AGG_START_TIMESTAMP_COL;

/**
 * This class is used to recreate in-memory data from the tables (Such as RDBMS) in incremental aggregation.
 * This ensures that the aggregation calculations are done correctly in case of server restart.
 */
public class IncrementalExecutorsInitialiser {
    private final List<TimePeriod.Duration> incrementalDurations;
    private final Map<TimePeriod.Duration, Table> aggregationTables;
    private final Map<TimePeriod.Duration, Executor> incrementalExecutorMap;

    private final boolean isDistributed;
    private final String shardId;

    private final StreamEventFactory streamEventFactory;

    private final SiddhiAppContext siddhiAppContext;
    private final Map<String, Table> tableMap;
    private final Map<String, Window> windowMap;
    private final Map<String, AggregationRuntime> aggregationMap;
    private String timeZone;
    private Long endOFLatestEventTimestamp = null;

    private boolean isInitialised;
    private boolean isReadOnly;
    private boolean isPersistedAggregation;

    public IncrementalExecutorsInitialiser(List<TimePeriod.Duration> incrementalDurations,
                                           Map<TimePeriod.Duration, Table> aggregationTables,
                                           Map<TimePeriod.Duration, Executor> incrementalExecutorMap,
                                           boolean isDistributed, String shardId, SiddhiAppContext siddhiAppContext,
                                           MetaStreamEvent metaStreamEvent, Map<String, Table> tableMap,
                                           Map<String, Window> windowMap,
                                           Map<String, AggregationRuntime> aggregationMap, String timeZone,
                                           boolean isReadOnly, boolean isPersistedAggregation) {
        this.timeZone = timeZone;
        this.incrementalDurations = incrementalDurations;
        this.aggregationTables = aggregationTables;
        this.incrementalExecutorMap = incrementalExecutorMap;

        this.isDistributed = isDistributed;
        this.shardId = shardId;

        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

        this.siddhiAppContext = siddhiAppContext;
        this.tableMap = tableMap;
        this.windowMap = windowMap;
        this.aggregationMap = aggregationMap;

        this.isInitialised = false;
        this.isReadOnly = isReadOnly;
        this.isPersistedAggregation = isPersistedAggregation;
    }

    public synchronized void initialiseExecutors() {
        if (this.isInitialised || isReadOnly) {
            // Only cleared when executors change from reading to processing state in one node deployment
            return;
        }
        Event[] events;
        Long lastData = null;

        // Get max(AGG_TIMESTAMP) from table corresponding to max duration
        Table tableForMaxDuration = aggregationTables.get(incrementalDurations.get(incrementalDurations.size() - 1));
        OnDemandQuery onDemandQuery = getOnDemandQuery(tableForMaxDuration, true, endOFLatestEventTimestamp);
        onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
        OnDemandQueryRuntime onDemandQueryRuntime = OnDemandQueryParser.parse(onDemandQuery, null,
                siddhiAppContext, tableMap, windowMap, aggregationMap);

        // Get latest event timestamp in tableForMaxDuration and get the end time of the aggregation record
        events = onDemandQueryRuntime.execute();
        if (events != null) {
            lastData = (Long) events[events.length - 1].getData(0);
            endOFLatestEventTimestamp = IncrementalTimeConverterUtil
                    .getNextEmitTime(lastData, incrementalDurations.get(incrementalDurations.size() - 1), timeZone);
        }

        if (isPersistedAggregation) {
            for (int i = incrementalDurations.size() - 1; i > 0; i--) {
                if (lastData != null && !IncrementalTimeConverterUtil.
                        isAggregationDataComplete(lastData, incrementalDurations.get(i), timeZone)) {
                    recreateState(lastData, incrementalDurations.get(i),
                            aggregationTables.get(incrementalDurations.get(i - 1)), i == 1);
                } else if (lastData == null) {
                    recreateState(null, incrementalDurations.get(i),
                            aggregationTables.get(incrementalDurations.get(i - 1)), i == 1);
                }
                if (i > 1) {
                    onDemandQuery = getOnDemandQuery(aggregationTables.get(incrementalDurations.get(i - 1)), true,
                            endOFLatestEventTimestamp);
                    onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
                    onDemandQueryRuntime = OnDemandQueryParser.parse(onDemandQuery, null,
                            siddhiAppContext, tableMap, windowMap, aggregationMap);
                    events = onDemandQueryRuntime.execute();
                    if (events != null) {
                        lastData = (Long) events[events.length - 1].getData(0);
                    } else {
                        lastData = null;
                    }
                }
            }
        } else {
            for (int i = incrementalDurations.size() - 1; i > 0; i--) {

                TimePeriod.Duration recreateForDuration = incrementalDurations.get(i);
                Executor incrementalExecutor = incrementalExecutorMap.get(recreateForDuration);


                // Get the table previous to the duration for which we need to recreate (e.g. if we want to recreate
                // for minute duration, take the second table [provided that aggregation is done for seconds])
                // This lookup is filtered by endOFLatestEventTimestamp
                Table recreateFromTable = aggregationTables.get(incrementalDurations.get(i - 1));

                onDemandQuery = getOnDemandQuery(recreateFromTable, false, endOFLatestEventTimestamp);
                onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
                onDemandQueryRuntime = OnDemandQueryParser.parse(onDemandQuery, null, siddhiAppContext,
                        tableMap, windowMap,
                        aggregationMap);
                events = onDemandQueryRuntime.execute();

                if (events != null) {
                    long referenceToNextLatestEvent = (Long) events[events.length - 1].getData(0);
                    endOFLatestEventTimestamp = IncrementalTimeConverterUtil
                            .getNextEmitTime(referenceToNextLatestEvent, incrementalDurations.get(i - 1), timeZone);

                    ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
                    for (Event event : events) {
                        StreamEvent streamEvent = streamEventFactory.newInstance();
                        streamEvent.setOutputData(event.getData());
                        complexEventChunk.add(streamEvent);
                    }
                    incrementalExecutor.execute(complexEventChunk);

                    if (i == 1) {
                        TimePeriod.Duration rootDuration = incrementalDurations.get(0);
                        Executor rootIncrementalExecutor = incrementalExecutorMap.get(rootDuration);
                        long emitTimeOfLatestEventInTable = IncrementalTimeConverterUtil.getNextEmitTime(
                                referenceToNextLatestEvent, rootDuration, timeZone);

                        rootIncrementalExecutor.setEmitTime(emitTimeOfLatestEventInTable);

                    }
                }
            }
        }
        this.isInitialised = true;
    }

    private void recreateState(Long lastData, TimePeriod.Duration recreateForDuration,
                               Table recreateFromTable, boolean isBeforeRoot) {
        Executor incrementalExecutor = incrementalExecutorMap.get(recreateForDuration);
        if (lastData != null) {
            endOFLatestEventTimestamp = IncrementalTimeConverterUtil
                    .getNextEmitTime(lastData, recreateForDuration, timeZone);
        }
        OnDemandQuery onDemandQuery = getOnDemandQuery(recreateFromTable, false, endOFLatestEventTimestamp);
        onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
        OnDemandQueryRuntime onDemandQueryRuntime = OnDemandQueryParser.parse(onDemandQuery, null, siddhiAppContext,
                tableMap, windowMap,
                aggregationMap);
        Event[] events = onDemandQueryRuntime.execute();
        if (events != null) {
            long referenceToNextLatestEvent = (Long) events[events.length - 1].getData(0);
            ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>();
            for (Event event : events) {
                StreamEvent streamEvent = streamEventFactory.newInstance();
                streamEvent.setOutputData(event.getData());
                complexEventChunk.add(streamEvent);
            }
            incrementalExecutor.execute(complexEventChunk);
            if (isBeforeRoot) {
                TimePeriod.Duration rootDuration = incrementalDurations.get(0);
                Executor rootIncrementalExecutor = incrementalExecutorMap.get(rootDuration);
                long emitTimeOfLatestEventInTable = IncrementalTimeConverterUtil.getNextEmitTime(
                        referenceToNextLatestEvent, rootDuration, timeZone);

                rootIncrementalExecutor.setEmitTime(emitTimeOfLatestEventInTable);

            }
        }
    }

    private OnDemandQuery getOnDemandQuery(Table table, boolean isLargestGranularity, Long endOFLatestEventTimestamp) {
        Selector selector = Selector.selector();
        if (isLargestGranularity) {
            selector = selector
                    .orderBy(
                            Expression.variable(AGG_START_TIMESTAMP_COL), OrderByAttribute.Order.DESC)
                    .limit(Expression.value(1));
        } else {
            selector = selector.orderBy(Expression.variable(AGG_START_TIMESTAMP_COL));
        }

        InputStore inputStore;
        if (!this.isDistributed) {
            if (endOFLatestEventTimestamp == null) {
                inputStore = InputStore.store(table.getTableDefinition().getId());
            } else {
                inputStore = InputStore.store(table.getTableDefinition().getId())
                        .on(Expression.compare(
                                Expression.variable(AGG_START_TIMESTAMP_COL),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(endOFLatestEventTimestamp)
                        ));
            }
        } else {
            if (endOFLatestEventTimestamp == null) {
                inputStore = InputStore.store(table.getTableDefinition().getId()).on(
                        Expression.compare(Expression.variable(AGG_SHARD_ID_COL), Compare.Operator.EQUAL,
                                Expression.value(shardId)));
            } else {
                inputStore = InputStore.store(table.getTableDefinition().getId()).on(
                        Expression.and(
                                Expression.compare(
                                        Expression.variable(AGG_SHARD_ID_COL),
                                        Compare.Operator.EQUAL,
                                        Expression.value(shardId)),
                                Expression.compare(
                                        Expression.variable(AGG_START_TIMESTAMP_COL),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.value(endOFLatestEventTimestamp))));
            }
        }

        return OnDemandQuery.query().from(inputStore).select(selector);
    }
}

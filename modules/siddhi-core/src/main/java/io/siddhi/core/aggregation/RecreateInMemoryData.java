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
import io.siddhi.core.query.StoreQueryRuntime;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.parser.StoreQueryParser;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to recreate in-memory data from the tables (Such as RDBMS) in incremental aggregation.
 * This ensures that the aggregation calculations are done correctly in case of server restart
 */
public class RecreateInMemoryData {
    private final List<TimePeriod.Duration> incrementalDurations;
    private final Map<TimePeriod.Duration, Table> aggregationTables;
    private final Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap;
    private final Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMapForPartitions;
    private final SiddhiAppContext siddhiAppContext;
    private final StreamEventFactory streamEventFactory;
    private final Map<String, Table> tableMap;
    private final Map<String, Window> windowMap;
    private final Map<String, AggregationRuntime> aggregationMap;
    private final String shardId;

    public RecreateInMemoryData(List<TimePeriod.Duration> incrementalDurations,
                                Map<TimePeriod.Duration, Table> aggregationTables,
                                Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap,
                                SiddhiAppContext siddhiAppContext, MetaStreamEvent metaStreamEvent, Map<String,
                                Table> tableMap, Map<String, Window> windowMap,
                                Map<String, AggregationRuntime> aggregationMap, String shardId,
                                Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMapForPartitions) {
        this.incrementalDurations = incrementalDurations;
        this.aggregationTables = aggregationTables;
        this.incrementalExecutorMap = incrementalExecutorMap;
        this.siddhiAppContext = siddhiAppContext;
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);
        this.tableMap = tableMap;
        this.windowMap = windowMap;
        this.aggregationMap = aggregationMap;
        this.shardId = shardId;
        this.incrementalExecutorMapForPartitions = incrementalExecutorMapForPartitions;
    }

    public void recreateInMemoryData(boolean refreshReadingExecutors) {
        IncrementalExecutor rootExecutor = incrementalExecutorMap.get(incrementalDurations.get(0));
        if (rootExecutor.isProcessingExecutor() && rootExecutor.getNextEmitTime() != -1 && !refreshReadingExecutors) {
            // If the getNextEmitTime is not -1, that implies that a snapshot of in-memory has already been
            // created. Hence this method does not have to be executed.
            //This is only true in a processing aggregation runtime
            return;
        }
        Event[] events;
        Long latestEventTimestamp = null;

        // Get all events from table corresponding to max duration
        Table tableForMaxDuration = aggregationTables.get(incrementalDurations.get(incrementalDurations.size() - 1));
        StoreQuery storeQuery;
        if (shardId == null || refreshReadingExecutors) {
            storeQuery = StoreQuery.query().from(InputStore.store(tableForMaxDuration.getTableDefinition().getId()))
                    .select(Selector.selector().orderBy(Expression.variable("AGG_TIMESTAMP")));
        } else {
            storeQuery = StoreQuery.query().from(
                    InputStore.store(tableForMaxDuration.getTableDefinition().getId())
                            .on(Expression.compare(Expression.variable("SHARD_ID"), Compare.Operator.EQUAL,
                                    Expression.value(shardId))))
                    .select(Selector.selector().orderBy(Expression.variable("AGG_TIMESTAMP")));
        }
        storeQuery.setType(StoreQuery.StoreQueryType.FIND);
        StoreQueryRuntime storeQueryRuntime = StoreQueryParser.parse(storeQuery, siddhiAppContext, tableMap, windowMap,
                aggregationMap);

        // Get latest event timestamp in tableForMaxDuration
        events = storeQueryRuntime.execute();
        if (events != null) {
            latestEventTimestamp = (Long) events[events.length - 1].getData(0);
        }

        for (int i = incrementalDurations.size() - 1; i > 0; i--) {
            TimePeriod.Duration recreateForDuration = incrementalDurations.get(i);
            IncrementalExecutor incrementalExecutor;

            if (refreshReadingExecutors) {
                incrementalExecutor = incrementalExecutorMapForPartitions.get(recreateForDuration);
            } else {
                incrementalExecutor = incrementalExecutorMap.get(recreateForDuration);
            }
            // Reset all executors when recreating again
            incrementalExecutor.clearExecutor();

            // Get the table previous to the duration for which we need to recreate (e.g. if we want to recreate
            // for minute duration, take the second table [provided that aggregation is done for seconds])
            Table recreateFromTable = aggregationTables.get(incrementalDurations.get(i - 1));

            if (shardId == null || refreshReadingExecutors) {
                storeQuery = StoreQuery.query().from(InputStore.store(recreateFromTable.getTableDefinition().getId()))
                        .select(Selector.selector().orderBy(Expression.variable("AGG_TIMESTAMP")));
            } else {
                storeQuery = StoreQuery.query().from(
                        InputStore.store(recreateFromTable.getTableDefinition().getId()).on(
                                Expression.compare(Expression.variable("SHARD_ID"), Compare.Operator.EQUAL,
                                        Expression.value(shardId))))
                        .select(Selector.selector().orderBy(Expression.variable("AGG_TIMESTAMP")));
            }

            storeQuery.setType(StoreQuery.StoreQueryType.FIND);
            storeQueryRuntime = StoreQueryParser.parse(storeQuery, siddhiAppContext, tableMap, windowMap,
                    aggregationMap);
            events = storeQueryRuntime.execute();

            if (events != null) {
                long referenceToNextLatestEvent = (Long) events[events.length - 1].getData(0);
                if (latestEventTimestamp != null) {
                    List<Event> eventsNewerThanLatestEventOfRecreateForDuration = new ArrayList<>();
                    for (Event event : events) {
                        // After getting the events from recreateFromTable, get the time bucket it belongs to,
                        // if each of these events were in the recreateForDuration. This helps to identify the events,
                        // which must be processed in-memory for recreateForDuration.
                        long timeBucketForNextDuration = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                                (Long) event.getData(0), recreateForDuration);
                        if (timeBucketForNextDuration > latestEventTimestamp) {
                            eventsNewerThanLatestEventOfRecreateForDuration.add(event);
                        }
                    }
                    events = eventsNewerThanLatestEventOfRecreateForDuration.toArray(
                            new Event[eventsNewerThanLatestEventOfRecreateForDuration.size()]);
                }

                latestEventTimestamp = referenceToNextLatestEvent;

                ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
                for (Event event : events) {
                    StreamEvent streamEvent = streamEventFactory.newInstance();
                    streamEvent.setOutputData(event.getData());
                    complexEventChunk.add(streamEvent);
                }
                incrementalExecutor.execute(complexEventChunk);

                if (i == 1) {
                    TimePeriod.Duration rootDuration = incrementalDurations.get(0);
                    IncrementalExecutor rootIncrementalExecutor;
                    if (refreshReadingExecutors) {
                        rootIncrementalExecutor = incrementalExecutorMapForPartitions.get(rootDuration);
                    } else {
                        rootIncrementalExecutor = incrementalExecutorMap.get(rootDuration);
                    }
                    long emitTimeOfLatestEventInTable = IncrementalTimeConverterUtil.getNextEmitTime(
                            latestEventTimestamp, rootDuration, null);

                    rootIncrementalExecutor.setValuesForInMemoryRecreateFromTable(emitTimeOfLatestEventInTable);

                }
            }
        }
    }
}

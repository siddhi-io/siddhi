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
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.util.IncrementalTimeConverterUtil;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the logic to aggregate data that is in-memory or in tables, in incremental data processing.
 * <p>
 * In-memory data is required to be aggregated when retrieving values from an aggregate.
 * Table data (tables used to persist incremental aggregates) needs to be aggregated when querying aggregate data from
 * a different server (apart from the server, which was used to define the aggregation).
 */
public class IncrementalDataAggregator {
    private final List<TimePeriod.Duration> incrementalDurations;
    private final TimePeriod.Duration aggregateForDuration;
    private final BaseIncrementalValueStore baseIncrementalValueStore;
    private final Map<String, BaseIncrementalValueStore> baseIncrementalValueStoreGroupByMap;

    public IncrementalDataAggregator(List<TimePeriod.Duration> incrementalDurations,
                                     TimePeriod.Duration aggregateForDuration, long oldestEventTimeStamp,
                                     List<ExpressionExecutor> baseExecutors,
                                     MetaStreamEvent metaStreamEvent, SiddhiAppContext siddhiAppContext,
                                     ExpressionExecutor shouldUpdateExpressionExecutor) {
        this.incrementalDurations = incrementalDurations;
        this.aggregateForDuration = aggregateForDuration;
        StreamEventPool streamEventPool = new StreamEventPool(metaStreamEvent, 10);
        this.baseIncrementalValueStore = new BaseIncrementalValueStore(oldestEventTimeStamp, baseExecutors,
                streamEventPool, siddhiAppContext, null, shouldUpdateExpressionExecutor);
        this.baseIncrementalValueStoreGroupByMap = new HashMap<>();
    }

    public ComplexEventChunk<StreamEvent> aggregateInMemoryData(
            Map<TimePeriod.Duration, IncrementalExecutor> incrementalExecutorMap) {
        int startIndex = incrementalDurations.indexOf(aggregateForDuration);
        for (int i = startIndex; i >= 0; i--) {
            TimePeriod.Duration duration = incrementalDurations.get(i);
            IncrementalExecutor incrementalExecutor = incrementalExecutorMap.get(duration);

            Map<String, BaseIncrementalValueStore> baseIncrementalValueStoreGroupByMap =
                    incrementalExecutor.getBaseIncrementalValueStoreGroupByMap();
            BaseIncrementalValueStore baseIncrementalValueStore = incrementalExecutor.getBaseIncrementalValueStore();

            if (baseIncrementalValueStoreGroupByMap != null) {
                for (Map.Entry<String, BaseIncrementalValueStore> entry :
                        baseIncrementalValueStoreGroupByMap.entrySet()) {
                    BaseIncrementalValueStore aBaseIncrementalValueStore = entry.getValue();
                    if (aBaseIncrementalValueStore.isProcessed()) {
                        processInMemoryAggregates(aBaseIncrementalValueStore.createStreamEvent(),
                                aBaseIncrementalValueStore.getTimestamp(), entry.getKey());
                    }
                }
            } else if (baseIncrementalValueStore.isProcessed()) {
                processInMemoryAggregates(baseIncrementalValueStore.createStreamEvent(),
                        baseIncrementalValueStore.getTimestamp(), null);
            }
        }
        return createEventChunkFromAggregatedData();
    }

    private void processInMemoryAggregates(StreamEvent streamEvent, long timestamp, String groupByKey) {
        long startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(timestamp,
                aggregateForDuration);
        synchronized (this) {
            if (groupByKey != null) {
                BaseIncrementalValueStore aBaseIncrementalValueStore =
                        this.baseIncrementalValueStoreGroupByMap.computeIfAbsent(
                                groupByKey, k -> baseIncrementalValueStore.cloneStore(k, startTimeOfAggregates));
                process(streamEvent, aBaseIncrementalValueStore);
            } else {
                process(streamEvent, this.baseIncrementalValueStore);
            }
        }
    }

    private void process(StreamEvent streamEvent, BaseIncrementalValueStore baseIncrementalValueStore) {
        List<ExpressionExecutor> expressionExecutors = baseIncrementalValueStore.getExpressionExecutors();
        boolean shouldUpdate = true;
        ExpressionExecutor shouldUpdateExpressionExecutor =
                baseIncrementalValueStore.getShouldUpdateExpressionExecutor();
        if (shouldUpdateExpressionExecutor != null) {
            shouldUpdate = ((boolean) shouldUpdateExpressionExecutor.execute(streamEvent));
        }

        for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
            if (shouldUpdate) {
                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
            } else {
                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                    baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
                }
            }
        }
        baseIncrementalValueStore.setProcessed(true);
    }

    private ComplexEventChunk<StreamEvent> createEventChunkFromAggregatedData() {
        ComplexEventChunk<StreamEvent> processedInMemoryEventChunk = new ComplexEventChunk<>(true);
        if (this.baseIncrementalValueStoreGroupByMap.size() == 0) {
            if (this.baseIncrementalValueStore.isProcessed()) {
                processedInMemoryEventChunk.add(this.baseIncrementalValueStore.createStreamEvent());
            }
        } else {
            for (Map.Entry<String, BaseIncrementalValueStore> entryAgainstGroupBy :
                    baseIncrementalValueStoreGroupByMap.entrySet()) {
                processedInMemoryEventChunk.add(entryAgainstGroupBy.getValue().createStreamEvent());
                }
            }
        return processedInMemoryEventChunk;
    }
}

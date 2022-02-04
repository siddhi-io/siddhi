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
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.parser.AggregationParser;
import io.siddhi.core.util.snapshot.state.PartitionSyncStateHolder;
import io.siddhi.core.util.snapshot.state.SingleSyncStateHolder;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the logic to aggregate data that is in-memory or in tables, in incremental data processing.
 * <p>
 * In-memory data is required to be aggregated when retrieving values from an aggregate.
 * Table data (tables used to persist incremental aggregates) needs to be aggregated when querying aggregate data from
 * a different server (apart from the server, which was used to define the aggregation).
 */
public class IncrementalDataAggregator {
    private static final Logger log = LogManager.getLogger(IncrementalDataAggregator.class);
    private final List<TimePeriod.Duration> incrementalDurations;
    private final TimePeriod.Duration durationToAggregate;

    private final long oldestEventTimestamp;
    private final List<ExpressionExecutor> baseExecutorsForFind;
    private final StateHolder valueStateHolder;
    private final StreamEvent resetEvent;
    private final StreamEventFactory streamEventFactory;
    private ExpressionExecutor shouldUpdateTimestamp;
    private String timeZone;

    public IncrementalDataAggregator(List<TimePeriod.Duration> incrementalDurations,
                                     TimePeriod.Duration durationToAggregate, long oldestEventTimestamp,
                                     List<ExpressionExecutor> baseExecutorsForFind,
                                     ExpressionExecutor shouldUpdateTimestamp, boolean groupBy,
                                     MetaStreamEvent metaStreamEvent, String timeZone) {
        this.timeZone = timeZone;
        this.incrementalDurations = incrementalDurations;
        this.durationToAggregate = durationToAggregate;

        this.oldestEventTimestamp = oldestEventTimestamp;
        this.baseExecutorsForFind = baseExecutorsForFind.subList(1, baseExecutorsForFind.size());
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;

        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

        if (groupBy) {
            this.valueStateHolder = new PartitionSyncStateHolder(() -> new ValueState());
        } else {
            this.valueStateHolder = new SingleSyncStateHolder(() -> new ValueState());
        }
        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventFactory.newInstance());

    }

    public ComplexEventChunk<StreamEvent> aggregateInMemoryData(
            Map<TimePeriod.Duration, Executor> incrementalExecutorMap) {
        int startIndex = incrementalDurations.indexOf(durationToAggregate);
        Set<String> groupByKeys = new HashSet<>();
        for (int k = startIndex; k >= 0; k--) {
            TimePeriod.Duration duration = incrementalDurations.get(k);
            Executor incrementalExecutor = incrementalExecutorMap.get(duration);
            if (incrementalExecutor instanceof IncrementalExecutor) {
                BaseIncrementalValueStore aBaseIncrementalValueStore = ((IncrementalExecutor) incrementalExecutor)
                        .getBaseIncrementalValueStore();
                Map<String, StreamEvent> groupedByEvents = aBaseIncrementalValueStore.getGroupedByEvents();
                for (Map.Entry<String, StreamEvent> eventEntry : groupedByEvents.entrySet()) {
                    long startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                            eventEntry.getValue().getTimestamp(), durationToAggregate, timeZone);
                    String groupByKey = eventEntry.getKey() + "-" + startTimeOfAggregates;
                    synchronized (this) {
                        groupByKeys.add(groupByKey);
                        SiddhiAppContext.startGroupByFlow(groupByKey);
                        ValueState state = (ValueState) valueStateHolder.getState();
                        try {
                            boolean shouldUpdate = true;
                            if (shouldUpdateTimestamp != null) {
                                shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(eventEntry.getValue()),
                                        state);
                            } else {
                                state.lastTimestamp = oldestEventTimestamp;
                            }
                            // keeping timestamp value location as null
                            for (int i = 0; i < baseExecutorsForFind.size(); i++) {
                                ExpressionExecutor expressionExecutor = baseExecutorsForFind.get(i);
                                if (shouldUpdate) {
                                    state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                                } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                                    state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                                }
                            }
                        } finally {
                            valueStateHolder.returnState(state);
                            SiddhiAppContext.stopGroupByFlow();
                        }
                    }
                }
            }
        }
        //clean all executors
        for (String groupByKey : groupByKeys) {
            SiddhiAppContext.startGroupByFlow(groupByKey);
            try {
                for (ExpressionExecutor expressionExecutor : baseExecutorsForFind) {
                    expressionExecutor.execute(resetEvent);
                }
            } finally {
                SiddhiAppContext.stopGroupByFlow();
            }
        }
        return getProcessedEventChunk();
    }


    private synchronized ComplexEventChunk<StreamEvent> getProcessedEventChunk() {
        ComplexEventChunk<StreamEvent> streamEventChunk = new ComplexEventChunk<>();
        Map<String, State> valueStoreMap = this.valueStateHolder.getAllGroupByStates();
        try {
            for (State aState : valueStoreMap.values()) {
                ValueState state = (ValueState) aState;
                StreamEvent streamEvent = streamEventFactory.newInstance();
                long timestamp = state.lastTimestamp;
                streamEvent.setTimestamp(timestamp);
                state.setValue(timestamp, 0);
                streamEvent.setOutputData(state.values);
                streamEventChunk.add(streamEvent);
            }
        } finally {
            this.valueStateHolder.returnGroupByStates(valueStoreMap);
        }
        return streamEventChunk;
    }


    private boolean shouldUpdate(Object data, ValueState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }

    class ValueState extends State {
        public long lastTimestamp;
        private Object[] values;

        public ValueState() {
            this.lastTimestamp = 0;
            this.values = new Object[baseExecutorsForFind.size() + 1];
        }

        @Override
        public boolean canDestroy() {
            return values == null && lastTimestamp == 0;
        }

        public void setValue(Object value, int position) {
            values[position] = value;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Values", values);
            state.put("LastTimestamp", lastTimestamp);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            values = (Object[]) state.get("Values");
            lastTimestamp = (Long) state.get("LastTimestamp");
        }

    }
}

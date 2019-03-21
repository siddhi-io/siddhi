/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.aggregation;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventPool;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.util.parser.AggregationParser;
import io.siddhi.core.util.snapshot.state.PartitionStateHolder;
import io.siddhi.core.util.snapshot.state.SingleStateHolder;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements logic to process aggregates(after retrieval from tables) that were aggregated
 * using external timestamp.
 */
public class IncrementalExternalTimestampDataAggregator {

    private final GroupByKeyGenerator groupByKeyGenerator;
    private final List<ExpressionExecutor> baseExecutors;
    private final StateHolder valueStateHolder;
    private final StreamEvent resetEvent;
    //    private final BaseIncrementalValueStore baseIncrementalValueStore;
//    private final ExpressionExecutor shouldUpdateExpressionExecutorForFind;
    private StreamEventPool streamEventPool;
    private final ExpressionExecutor shouldUpdateTimestamp;

    public IncrementalExternalTimestampDataAggregator(List<ExpressionExecutor> baseExecutors,
                                                      GroupByKeyGenerator groupByKeyGenerator,
                                                      MetaStreamEvent metaStreamEvent,
                                                      SiddhiQueryContext siddhiQueryContext,
                                                      ExpressionExecutor shouldUpdateTimestamp) {
        this.baseExecutors = baseExecutors.subList(1, baseExecutors.size());
        streamEventPool = new StreamEventPool(metaStreamEvent, 10);
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;

//        List<ExpressionExecutor> expressionExecutorsWithoutTime = baseExecutors.subList(1, baseExecutors.size());
//        this.baseIncrementalValueStore = new BaseIncrementalValueStore(
//                expressionExecutorsWithoutTime, streamEventPool, siddhiQueryContext, null,
//                shouldUpdateExpressionExecutorForFind, -1, groupBy, true);
        this.groupByKeyGenerator = groupByKeyGenerator;
        if (groupByKeyGenerator != null) {
            this.valueStateHolder = new PartitionStateHolder(() -> new ValueState());
        } else {
            this.valueStateHolder = new SingleStateHolder(() -> new ValueState());
        }
        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventPool.borrowEvent());

//        this.shouldUpdateExpressionExecutorForFind = shouldUpdateExpressionExecutorForFind;
    }

    public ComplexEventChunk<StreamEvent> aggregateData(ComplexEventChunk<StreamEvent> retrievedData) {

        Set<String> groupByKeys = new HashSet<>();
        while (retrievedData.hasNext()) {
            StreamEvent streamEvent = retrievedData.next();
            String groupByKey = groupByKeyGenerator.constructEventKey(streamEvent);
            groupByKeys.add(groupByKey);
            SiddhiAppContext.startGroupByFlow(groupByKey);
            ValueState state = (ValueState) valueStateHolder.getState();

//            try {
//            BaseIncrementalValueStore baseIncrementalValueStore = baseIncrementalValueGroupByStore
//                    .computeIfAbsent(
//                            groupByKey, k -> this.baseIncrementalValueStore.cloneStore(k, -1)
//                    );
//                process(streamEvent, baseIncrementalValueStore);
//                process(streamEvent);

            try {
                boolean shouldUpdate = true;
                if (shouldUpdateTimestamp != null) {
                    shouldUpdate = (boolean) shouldUpdate(shouldUpdateTimestamp.execute(streamEvent), state);
                }
                for (int i = 0; i < baseExecutors.size(); i++) { // keeping timestamp value location as null
                    if (shouldUpdate) {
                        ExpressionExecutor expressionExecutor = baseExecutors.get(i);
                        state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                    } else {
                        ExpressionExecutor expressionExecutor = baseExecutors.get(i);
                        if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                            state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                        }
                    }
                }
            } finally {
                valueStateHolder.returnState(state);
                SiddhiAppContext.stopGroupByFlow();
            }
//            } finally {
//                SiddhiAppContext.stopGroupByFlow();
//            }
        }

        //clean all executors
        for (String groupByKey : groupByKeys) {
            SiddhiAppContext.startGroupByFlow(groupByKey);
            try {
                for (ExpressionExecutor expressionExecutor : baseExecutors) {
                    expressionExecutor.execute(resetEvent);
                }
            } finally {
                SiddhiAppContext.stopGroupByFlow();
            }
        }
        return createEventChunkFromAggregatedData();
    }

//    private void process(StreamEvent streamEvent, BaseIncrementalValueStore baseIncrementalValueStore) {
//        List<ExpressionExecutor> expressionExecutors = baseIncrementalValueStore.getExpressionExecutors();
//        boolean shouldUpdate = true;
//        ExpressionExecutor shouldUpdateExpressionExecutor =
//                baseIncrementalValueStore.getShouldUpdateExpressionExecutor();
//        if (shouldUpdateExpressionExecutor != null) {
//            shouldUpdate = ((boolean) shouldUpdateExpressionExecutor.execute(streamEvent));
//        }
//        baseIncrementalValueStore.setProcessed(true);
//        for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
//            if (shouldUpdate) {
//                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
//                baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
//            } else {
//                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
//                if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
//                    baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
//                }
//            }
//        }
//    }

//    private void process(StreamEvent streamEvent) {
//        IncrementalDataAggregator.ValueState state = (IncrementalDataAggregator.ValueState)
// valueStateHolder.getState();
//        try {
//            boolean shouldUpdate = true;
//            if (shouldUpdateTimestamp != null) {
//                shouldUpdate = ((boolean) shouldUpdate(shouldUpdateTimestamp.execute(streamEvent), );
//            }
//            for (int i = 0; i < baseExecutors.size(); i++) { // keeping timestamp value location as null
//                if (shouldUpdate) {
//                    ExpressionExecutor expressionExecutor = baseExecutors.get(i);
//                    baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
//                } else {
//                    ExpressionExecutor expressionExecutor = baseExecutors.get(i);
//                    if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
//                        baseIncrementalValueStore.setValue(expressionExecutor.execute(streamEvent), i + 1);
//                    }
//                }
//            }
//        } finally {
//            valueStateHolder.returnState(state);
//        }
//    }

    private Object shouldUpdate(Object data, ValueState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }


    private ComplexEventChunk<StreamEvent> createEventChunkFromAggregatedData() {
//        ComplexEventChunk<StreamEvent> processedInMemoryEventChunk = new ComplexEventChunk<>(true);
//        return baseIncrementalValueStore.getProcessedEventChunk();
//        for (Map.Entry<String, BaseIncrementalValueStore> entryAgainstTime :
//                baseIncrementalValueGroupByStore.entrySet()) {
//            processedInMemoryEventChunk.add(entryAgainstTime.getValue().createStreamEvent());
//        }
//        return processedInMemoryEventChunk;

        ComplexEventChunk<StreamEvent> streamEventChunk = new ComplexEventChunk<>(true);
        Map<String, State> valueStoreMap = this.valueStateHolder.getAllStates();
        try {
            for (State aState : valueStoreMap.values()) {
                ValueState state = (ValueState) aState;
                StreamEvent streamEvent = streamEventPool.borrowEvent();
                long timestamp = state.lastTimestamp;
                streamEvent.setTimestamp(timestamp);
                state.setValue(timestamp, 0);
                streamEvent.setOutputData(state.values);
                streamEventChunk.add(streamEvent);
            }
        } finally {
            this.valueStateHolder.returnStates(valueStoreMap);
        }
        return streamEventChunk;
    }

    class ValueState extends State {
        private Object[] values;
        private long lastTimestamp = 0;

        public ValueState() {
            this.values = new Object[baseExecutors.size() + 1];
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

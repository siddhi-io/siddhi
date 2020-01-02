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
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.util.parser.AggregationParser;
import io.siddhi.core.util.snapshot.state.PartitionSyncStateHolder;
import io.siddhi.core.util.snapshot.state.SingleSyncStateHolder;
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
public class OutOfOrderEventsDataAggregator {

    private final GroupByKeyGenerator groupByKeyGenerator;
    private final StateHolder valueStateHolder;
    private final StreamEvent resetEvent;
    private final List<ExpressionExecutor> baseExecutors;
    private final ExpressionExecutor shouldUpdateTimestamp;
    private StreamEventFactory streamEventFactory;

    public OutOfOrderEventsDataAggregator(List<ExpressionExecutor> baseExecutors,
                                          ExpressionExecutor shouldUpdateTimestamp,
                                          GroupByKeyGenerator groupByKeyGenerator, MetaStreamEvent metaStreamEvent) {

        this.baseExecutors = baseExecutors.subList(1, baseExecutors.size());
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;

        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

        this.groupByKeyGenerator = groupByKeyGenerator;
        if (groupByKeyGenerator != null) {
            this.valueStateHolder = new PartitionSyncStateHolder(() -> new ValueState());
        } else {
            this.valueStateHolder = new SingleSyncStateHolder(() -> new ValueState());
        }

        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventFactory.newInstance());
    }

    public ComplexEventChunk<StreamEvent> aggregateData(ComplexEventChunk<StreamEvent> retrievedData) {

        Set<String> groupByKeys = new HashSet<>();
        while (retrievedData.hasNext()) {
            StreamEvent streamEvent = retrievedData.next();
            String groupByKey = groupByKeyGenerator.constructEventKey(streamEvent);
            groupByKeys.add(groupByKey);
            SiddhiAppContext.startGroupByFlow(groupByKey);
            synchronized (this) {
                ValueState state = (ValueState) valueStateHolder.getState();
                try {
                    boolean shouldUpdate = true;
                    if (shouldUpdateTimestamp != null) {
                        shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(streamEvent), state);
                    }
                    for (int i = 0; i < baseExecutors.size(); i++) { // keeping timestamp value location as null
                        ExpressionExecutor expressionExecutor = baseExecutors.get(i);
                        if (shouldUpdate) {
                            state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                        } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                            state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                        }
                    }
                } finally {
                    valueStateHolder.returnState(state);
                    SiddhiAppContext.stopGroupByFlow();
                }
            }
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

    private boolean shouldUpdate(Object data, ValueState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }


    private synchronized ComplexEventChunk<StreamEvent> createEventChunkFromAggregatedData() {
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

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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.util.snapshot.state.PartitionSyncStateHolder;
import io.siddhi.core.util.snapshot.state.SingleSyncStateHolder;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store for maintaining the base values related to incremental aggregation. (e.g. for average,
 * the base incremental values would be sum and count. The timestamp too is stored here.
 */
public class BaseIncrementalValueStore {
    private static final Logger log = LogManager.getLogger(BaseIncrementalValueStore.class);
    private StateHolder<ValueState> valueStateHolder;
    private StateHolder<StoreState> storeStateHolder;

    private long initialTimestamp;
    private List<ExpressionExecutor> expressionExecutors;
    private ExpressionExecutor shouldUpdateTimestamp;

    private StreamEventFactory streamEventFactory;

    public BaseIncrementalValueStore(String aggregatorName, long initialTimestamp,
                                     List<ExpressionExecutor> expressionExecutors,
                                     ExpressionExecutor shouldUpdateTimestamp, StreamEventFactory streamEventFactory,
                                     SiddhiQueryContext siddhiQueryContext, boolean groupBy, boolean local) {

        this.initialTimestamp = initialTimestamp;
        this.expressionExecutors = expressionExecutors;
        this.shouldUpdateTimestamp = shouldUpdateTimestamp;
        this.streamEventFactory = streamEventFactory;

        if (!local) {
            this.valueStateHolder = siddhiQueryContext.generateStateHolder(aggregatorName + "-" +
                    this.getClass().getName() + "-value", groupBy, () -> new ValueState());
            this.storeStateHolder = siddhiQueryContext.generateStateHolder(aggregatorName + "-" +
                    this.getClass().getName(), false, () -> new StoreState());
        } else {
            this.valueStateHolder = new PartitionSyncStateHolder(() -> new ValueState());
            this.storeStateHolder = new SingleSyncStateHolder(() -> new StoreState());
        }
    }

    public synchronized void clearValues(long startTimeOfNewAggregates, StreamEvent resetEvent) {
        this.initialTimestamp = startTimeOfNewAggregates;
        setTimestamp(startTimeOfNewAggregates);
        setProcessed(false);
        this.valueStateHolder.cleanGroupByStates();
    }

    public List<ExpressionExecutor> getExpressionExecutors() {
        return expressionExecutors;
    }

    public synchronized boolean isProcessed() {
        StoreState state = this.storeStateHolder.getState();
        try {
            return state.isProcessed;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private void setProcessed(boolean isProcessed) {
        StoreState state = this.storeStateHolder.getState();
        try {
            state.isProcessed = isProcessed;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private long getTimestamp() {
        StoreState state = this.storeStateHolder.getState();
        try {
            return state.timestamp;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    private void setTimestamp(long timestamp) {
        StoreState state = this.storeStateHolder.getState();
        try {
            state.timestamp = timestamp;
        } finally {
            this.storeStateHolder.returnState(state);
        }
    }

    public synchronized Map<String, StreamEvent> getGroupedByEvents() {
        Map<String, StreamEvent> groupedByEvents = new HashMap<>();

        if (isProcessed()) {
            Map<String, ValueState> baseIncrementalValueStoreMap = this.valueStateHolder.getAllGroupByStates();
            try {
                for (Map.Entry<String, ValueState> state : baseIncrementalValueStoreMap.entrySet()) {
                    StreamEvent streamEvent = streamEventFactory.newInstance();
                    long timestamp = getTimestamp();
                    streamEvent.setTimestamp(timestamp);
                    state.getValue().setValue(timestamp, 0);
                    streamEvent.setOutputData(state.getValue().values);
                    groupedByEvents.put(state.getKey(), streamEvent);
                }
            } finally {
                this.valueStateHolder.returnGroupByStates(baseIncrementalValueStoreMap);
            }
        }
        return groupedByEvents;
    }

    public synchronized void process(StreamEvent streamEvent) {
        ValueState state = valueStateHolder.getState();
        try {
            boolean shouldUpdate = true;
            if (shouldUpdateTimestamp != null) {
                shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(streamEvent), state);
            }
            for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
                ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                if (shouldUpdate) {
                    state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                    state.setValue(expressionExecutor.execute(streamEvent), i + 1);
                }
            }
            setProcessed(true);
        } finally {
            valueStateHolder.returnState(state);
        }
    }

    public synchronized void process(Map<String, StreamEvent> groupedByEvents) {
        for (Map.Entry<String, StreamEvent> eventEntry : groupedByEvents.entrySet()) {
            synchronized (this) {
                SiddhiAppContext.startGroupByFlow(eventEntry.getKey() + "-" +
                        eventEntry.getValue().getTimestamp());
                ValueState state = valueStateHolder.getState();
                try {
                    boolean shouldUpdate = true;
                    if (shouldUpdateTimestamp != null) {
                        shouldUpdate = shouldUpdate(shouldUpdateTimestamp.execute(eventEntry.getValue()), state);
                    }
                    for (int i = 0; i < expressionExecutors.size(); i++) { // keeping timestamp value location as null
                        ExpressionExecutor expressionExecutor = expressionExecutors.get(i);
                        if (shouldUpdate) {
                            state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                        } else if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
                            state.setValue(expressionExecutor.execute(eventEntry.getValue()), i + 1);
                        }
                    }
                    setProcessed(true);
                } finally {
                    valueStateHolder.returnState(state);
                    SiddhiAppContext.stopGroupByFlow();
                }
            }
        }
    }

    private boolean shouldUpdate(Object data, ValueState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }

    class StoreState extends State {
        private long timestamp;
        private boolean isProcessed = false;

        public StoreState() {
            this.timestamp = initialTimestamp;
        }

        @Override
        public boolean canDestroy() {
            return !isProcessed;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Timestamp", timestamp);
            state.put("IsProcessed", isProcessed);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            timestamp = (long) state.get("Timestamp");
            isProcessed = (boolean) state.get("IsProcessed");
        }

        public void setIfAbsentTimestamp(long timestamp) {
            if (this.timestamp == -1) {
                this.timestamp = timestamp;
            }
        }
    }

    class ValueState extends State {
        public long lastTimestamp = 0;
        private Object[] values;

        public ValueState() {
            this.values = new Object[expressionExecutors.size() + 1];
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

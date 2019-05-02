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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.AggregationParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Incremental executor class which is responsible for performing incremental aggregation.
 */
public class IncrementalExecutor implements Executor {
    private static final Logger LOG = Logger.getLogger(IncrementalExecutor.class);

    private final StreamEvent resetEvent;
    private final ExpressionExecutor timestampExpressionExecutor;
    private final StateHolder<ExecutorState> stateHolder;
    private final String aggregatorName;
    private final String siddhiAppName;
    private TimePeriod.Duration duration;
    private Table table;
    private GroupByKeyGenerator groupByKeyGenerator;
    private StreamEventFactory streamEventFactory;
    private Executor next;
    private Scheduler scheduler;
    private boolean isRoot;
    private boolean isProcessingExecutor;
    private ExecutorService executorService;

    private BaseIncrementalValueStore baseIncrementalValueStore = null;

    public IncrementalExecutor(TimePeriod.Duration duration, List<ExpressionExecutor> processExpressionExecutors,
                               GroupByKeyGenerator groupByKeyGenerator, MetaStreamEvent metaStreamEvent,
                               IncrementalExecutor child, boolean isRoot, Table table,
                               SiddhiQueryContext siddhiQueryContext, String aggregatorName,
                               ExpressionExecutor shouldUpdateTimestamp) {
        this.duration = duration;
        this.next = child;
        this.isRoot = isRoot;
        this.table = table;
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);
        this.timestampExpressionExecutor = processExpressionExecutors.remove(0);
        this.isProcessingExecutor = false;
        this.groupByKeyGenerator = groupByKeyGenerator;
        this.baseIncrementalValueStore = new BaseIncrementalValueStore(processExpressionExecutors, streamEventFactory,
                siddhiQueryContext, aggregatorName, shouldUpdateTimestamp, -1, true, false);
        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventFactory.newInstance());
        setNextExecutor(child);

        this.stateHolder = siddhiQueryContext.generateStateHolder(
                aggregatorName + "-" + this.getClass().getName(), false, () -> new ExecutorState());
        this.executorService = Executors.newSingleThreadExecutor();
        this.aggregatorName = aggregatorName;
        this.siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void execute(ComplexEventChunk streamEventChunk) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Event Chunk received by " + this.duration + " incremental executor: " +
                    streamEventChunk.toString());
        }
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = (StreamEvent) streamEventChunk.next();
            streamEventChunk.remove();
            ExecutorState executorState = stateHolder.getState();
            try {
                long timestamp = getTimestamp(streamEvent, executorState);
                executorState.startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                        timestamp, duration);
                if (timestamp >= executorState.nextEmitTime) {
                    executorState.nextEmitTime = IncrementalTimeConverterUtil.getNextEmitTime(
                            timestamp, duration, null);
                    dispatchAggregateEvents(executorState.startTimeOfAggregates);
                    sendTimerEvent(executorState);
                }
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    processAggregates(streamEvent, executorState);
                }
            } finally {
                stateHolder.returnState(executorState);
            }
        }
    }

    private void sendTimerEvent(ExecutorState executorState) {
        if (getNextExecutor() != null) {
            StreamEvent timerEvent = streamEventFactory.newInstance();
            timerEvent.setType(ComplexEvent.Type.TIMER);
            timerEvent.setTimestamp(executorState.startTimeOfAggregates);
            ComplexEventChunk<StreamEvent> timerStreamEventChunk = new ComplexEventChunk<>(true);
            timerStreamEventChunk.add(timerEvent);
            next.execute(timerStreamEventChunk);
        }
    }

    private long getTimestamp(StreamEvent streamEvent, ExecutorState executorState) {
        long timestamp;
        if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
            timestamp = (long) timestampExpressionExecutor.execute(streamEvent);
            if (isRoot && !executorState.timerStarted) {
                scheduler.notifyAt(IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, null));
                executorState.timerStarted = true;
            }
        } else {
            timestamp = streamEvent.getTimestamp();
            if (isRoot) {
                // Scheduling is done by root incremental executor only
                scheduler.notifyAt(IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, null));
            }
        }
        return timestamp;
    }

    @Override
    public Executor getNextExecutor() {
        return next;
    }

    @Override
    public void setNextExecutor(Executor nextExecutor) {
        next = nextExecutor;
    }

    private void processAggregates(StreamEvent streamEvent, ExecutorState executorState) {
        synchronized (this) {
            if (groupByKeyGenerator != null) {
                try {
                    String groupedByKey = groupByKeyGenerator.constructEventKey(streamEvent);
                    SiddhiAppContext.startGroupByFlow(groupedByKey);
                    baseIncrementalValueStore.process(streamEvent);
                } finally {
                    SiddhiAppContext.stopGroupByFlow();
                }
            } else {
                baseIncrementalValueStore.process(streamEvent);
            }
        }
    }


    private void dispatchAggregateEvents(long startTimeOfNewAggregates) {
        dispatchEvent(startTimeOfNewAggregates, baseIncrementalValueStore);
    }

    private void dispatchEvent(long startTimeOfNewAggregates, BaseIncrementalValueStore aBaseIncrementalValueStore) {
        if (aBaseIncrementalValueStore.isProcessed()) {
            Map<String, StreamEvent> streamEventMap = aBaseIncrementalValueStore.getGroupedByEvents();
            ComplexEventChunk<StreamEvent> eventChunk = new ComplexEventChunk<>(true);
            for (StreamEvent event : streamEventMap.values()) {
                eventChunk.add(event);
            }
            Map<String, StreamEvent> tableStreamEventMap = aBaseIncrementalValueStore.getGroupedByEvents();
            ComplexEventChunk<StreamEvent> tableEventChunk = new ComplexEventChunk<>(true);
            for (StreamEvent event : tableStreamEventMap.values()) {
                tableEventChunk.add(event);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Event dispatched by " + this.duration + " incremental executor: " + eventChunk.toString());
            }
            if (isProcessingExecutor) {
                executorService.execute(() -> {
                            try {
                                table.addEvents(tableEventChunk, streamEventMap.size());
                            } catch (Throwable t) {
                                LOG.error("Exception occurred at siddhi app '" + this.siddhiAppName +
                                        "' when performing table writes of aggregation '" + this.aggregatorName +
                                        "' for duration '" + this.duration + "'. This should be investigated as this " +
                                        "can cause accuracy loss.", t);
                            }
                        }
                    );
            }
            if (getNextExecutor() != null) {
                next.execute(eventChunk);
            }
        }
        cleanBaseIncrementalValueStore(startTimeOfNewAggregates, aBaseIncrementalValueStore);
    }

    private void cleanBaseIncrementalValueStore(long startTimeOfNewAggregates,
                                                BaseIncrementalValueStore baseIncrementalValueStore) {
        baseIncrementalValueStore.clearValues(startTimeOfNewAggregates, resetEvent);
        for (ExpressionExecutor expressionExecutor : baseIncrementalValueStore.getExpressionExecutors()) {
            expressionExecutor.execute(resetEvent);
        }
    }

    BaseIncrementalValueStore getBaseIncrementalValueStore() {
        return baseIncrementalValueStore;
    }

    public long getAggregationStartTimestamp() {
        ExecutorState state = stateHolder.getState();
        try {
            return state.startTimeOfAggregates;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public long getNextEmitTime() {
        ExecutorState state = stateHolder.getState();
        try {
            return state.nextEmitTime;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void setValuesForInMemoryRecreateFromTable(long emitTimeOfLatestEventInTable) {
        ExecutorState state = stateHolder.getState();
        try {
            state.nextEmitTime = emitTimeOfLatestEventInTable;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public boolean isProcessingExecutor() {
        return isProcessingExecutor;
    }

    public void setProcessingExecutor(boolean processingExecutor) {
        isProcessingExecutor = processingExecutor;
    }

    public void clearExecutor() {
        cleanBaseIncrementalValueStore(-1, this.baseIncrementalValueStore);
    }

    class ExecutorState extends State {
        private long nextEmitTime = -1;
        private long startTimeOfAggregates = -1;
        private boolean timerStarted = false;
        private boolean canDestroy = false;

        @Override
        public boolean canDestroy() {
            return canDestroy;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("NextEmitTime", nextEmitTime);
            state.put("StartTimeOfAggregates", startTimeOfAggregates);
            state.put("TimerStarted", timerStarted);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            nextEmitTime = (long) state.get("NextEmitTime");
            startTimeOfAggregates = (long) state.get("StartTimeOfAggregates");
            timerStarted = (boolean) state.get("TimerStarted");
        }

        public void setCanDestroy(boolean canDestroy) {
            this.canDestroy = canDestroy;
        }
    }
}

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Incremental executor class which is responsible for performing incremental aggregation.
 */
public class IncrementalExecutor implements Executor {
    private static final Logger LOG = LogManager.getLogger(IncrementalExecutor.class);

    private final String aggregatorName;
    private final StreamEvent resetEvent;
    private final ExpressionExecutor timestampExpressionExecutor;
    private final StateHolder<ExecutorState> stateHolder;
    private final String siddhiAppName;
    private final Lock lock = new ReentrantLock();
    boolean waitUntillprocessFinish = false;
    private TimePeriod.Duration duration;
    private Table table;
    private boolean isRoot;
    private boolean isProcessingExecutor;
    private Executor next;
    private GroupByKeyGenerator groupByKeyGenerator;
    private StreamEventFactory streamEventFactory;
    private Scheduler scheduler;
    private ExecutorService executorService;
    private String timeZone;
    private BaseIncrementalValueStore baseIncrementalValueStore;


    public IncrementalExecutor(String aggregatorName, TimePeriod.Duration duration,
                               List<ExpressionExecutor> processExpressionExecutors,
                               ExpressionExecutor shouldUpdateTimestamp, GroupByKeyGenerator groupByKeyGenerator,
                               boolean isRoot, Table table, Executor child,
                               SiddhiQueryContext siddhiQueryContext, MetaStreamEvent metaStreamEvent,
                               String timeZone, boolean waitUntillprocessFinish) {
        this.timeZone = timeZone;
        this.aggregatorName = aggregatorName;
        this.duration = duration;
        this.isRoot = isRoot;
        this.table = table;
        this.next = child;

        this.waitUntillprocessFinish = waitUntillprocessFinish;
        this.timestampExpressionExecutor = processExpressionExecutors.remove(0);
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

        this.groupByKeyGenerator = groupByKeyGenerator;
        this.baseIncrementalValueStore = new BaseIncrementalValueStore(aggregatorName, -1,
                processExpressionExecutors, shouldUpdateTimestamp, streamEventFactory, siddhiQueryContext, true,
                false);
        this.resetEvent = AggregationParser.createRestEvent(metaStreamEvent, streamEventFactory.newInstance());
        setNextExecutor(child);

        this.siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                aggregatorName + "-" + this.getClass().getName(), false, () -> new ExecutorState());
        this.executorService = Executors.newSingleThreadExecutor();

        this.isProcessingExecutor = false;

    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public synchronized void execute(ComplexEventChunk streamEventChunk) {
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
                long startTime = executorState.startTimeOfAggregates;
                executorState.startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                        timestamp, duration, timeZone);
                if (timestamp >= executorState.nextEmitTime) {
                    executorState.nextEmitTime = IncrementalTimeConverterUtil.getNextEmitTime(
                            timestamp, duration, timeZone);
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
            ComplexEventChunk<StreamEvent> timerStreamEventChunk = new ComplexEventChunk<>();
            timerStreamEventChunk.add(timerEvent);
            next.execute(timerStreamEventChunk);
        }
    }

    private long getTimestamp(StreamEvent streamEvent, ExecutorState executorState) {
        long timestamp;
        if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
            timestamp = (long) timestampExpressionExecutor.execute(streamEvent);
            if (isRoot && !executorState.timerStarted) {
                scheduler.notifyAt(IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, timeZone));
                executorState.timerStarted = true;
            }
        } else {
            timestamp = streamEvent.getTimestamp();
            if (isRoot) {
                // Scheduling is done by root incremental executor only
                scheduler.notifyAt(IncrementalTimeConverterUtil.getNextEmitTime(timestamp, duration, timeZone));
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
        AtomicBoolean isProcessFinished = new AtomicBoolean(false);
        if (aBaseIncrementalValueStore.isProcessed()) {
            Map<String, StreamEvent> streamEventMap = aBaseIncrementalValueStore.getGroupedByEvents();
            ComplexEventChunk<StreamEvent> eventChunk = new ComplexEventChunk<>();
            for (StreamEvent event : streamEventMap.values()) {
                eventChunk.add(event);
            }
            Map<String, StreamEvent> tableStreamEventMap = aBaseIncrementalValueStore.getGroupedByEvents();
            ComplexEventChunk<StreamEvent> tableEventChunk = new ComplexEventChunk<>();
            for (StreamEvent event : tableStreamEventMap.values()) {
                tableEventChunk.add(event);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Event dispatched by aggregation " + aggregatorName + " for duration " + this.duration);
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
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Dropping Event chunk - \"" + eventChunk.toString() + "\"");
                        }
                    } finally {
                        isProcessFinished.set(true);
                    }
                });

            }
            if (waitUntillprocessFinish) {
                try {
                    while (!isProcessFinished.get()) {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    LOG.error("Error occurred while waiting until table update task finishes for duration " +
                            duration + "in aggregation " + aggregatorName, e);
                }
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

    public void setEmitTime(long emitTimeOfLatestEventInTable) {
        ExecutorState state = stateHolder.getState();
        try {
            state.nextEmitTime = emitTimeOfLatestEventInTable;
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void setProcessingExecutor(boolean processingExecutor) {
        isProcessingExecutor = processingExecutor;
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

/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.aggregation.persistedaggregation;

import io.siddhi.core.aggregation.Executor;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Incremental Executor implementation class for Persisted Aggregation
 **/
public class PersistedIncrementalExecutor implements Executor {
    private static final Logger log = LogManager.getLogger(PersistedIncrementalExecutor.class);

    private final ExpressionExecutor timestampExpressionExecutor;
    private final StateHolder<ExecutorState> stateHolder;
    private TimePeriod.Duration duration;
    private Executor next;
    private StreamEventFactory streamEventFactory;
    private String timeZone;
    private Processor cudStreamProcessor;
    private boolean isProcessingExecutor;
    private LinkedBlockingQueue<QueuedCudStreamProcessor> cudStreamProcessorQueue;
    private String aggregatorName;

    public PersistedIncrementalExecutor(String aggregatorName, TimePeriod.Duration duration,
                                        List<ExpressionExecutor> processExpressionExecutors,
                                        Executor child, SiddhiQueryContext siddhiQueryContext,
                                        MetaStreamEvent metaStreamEvent, String timeZone,
                                        Processor cudStreamProcessor, LinkedBlockingQueue<QueuedCudStreamProcessor>
                                                cudStreamProcessorQueue) {
        this.timeZone = timeZone;
        this.duration = duration;
        this.next = child;
        this.cudStreamProcessor = cudStreamProcessor;

        this.aggregatorName = aggregatorName;
        this.timestampExpressionExecutor = processExpressionExecutors.remove(0);
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);
        setNextExecutor(child);

        this.stateHolder = siddhiQueryContext.generateStateHolder(
                aggregatorName + "-" + this.getClass().getName(), false,
                () -> new ExecutorState());
        this.isProcessingExecutor = false;
        this.cudStreamProcessorQueue = cudStreamProcessorQueue;
    }

    @Override
    public void execute(ComplexEventChunk streamEventChunk) {
        if (log.isDebugEnabled()) {
            log.debug("Event Chunk received by the Aggregation " + aggregatorName + " for duration " + this.duration +
                    " will be dropped since persisted aggregation has been scheduled ");
        }
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = (StreamEvent) streamEventChunk.next();
            streamEventChunk.remove();
            ExecutorState executorState = stateHolder.getState();
            try {
                long timestamp = getTimestamp(streamEvent);
                if (timestamp >= executorState.nextEmitTime) {
                    log.debug("Next EmitTime: " + executorState.nextEmitTime + ", Current Time: " + timestamp);
                    long emittedTime = executorState.nextEmitTime;
                    long startedTime = executorState.startTimeOfAggregates;
                    executorState.startTimeOfAggregates = IncrementalTimeConverterUtil.getStartTimeOfAggregates(
                            timestamp, duration, timeZone);
                    executorState.nextEmitTime = IncrementalTimeConverterUtil.getNextEmitTime(
                            timestamp, duration, timeZone);
                    dispatchAggregateEvents(startedTime, emittedTime, timeZone);
                    sendTimerEvent(executorState);
                }
            } finally {
                stateHolder.returnState(executorState);
            }
        }
    }

    private void dispatchAggregateEvents(long startTimeOfNewAggregates, long emittedTime, String timeZone) {
        if (emittedTime != -1) {
            dispatchEvent(startTimeOfNewAggregates, emittedTime, timeZone);
        }
    }

    private void dispatchEvent(long startTimeOfNewAggregates, long emittedTime, String timeZone) {
        ZonedDateTime startTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimeOfNewAggregates),
                ZoneId.of(timeZone));
        ZonedDateTime endTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(emittedTime),
                ZoneId.of(timeZone));
        log.info("Aggregation event dispatched for the duration " + duration + " for aggregation " + aggregatorName +
                " to aggregate data from " + startTime + " to " + endTime + " ");
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        StreamEvent streamEvent = streamEventFactory.newInstance();
        streamEvent.setType(ComplexEvent.Type.CURRENT);
        streamEvent.setTimestamp(emittedTime);
        List<Object> outputDataList = new ArrayList<>();
        outputDataList.add(startTimeOfNewAggregates);
        outputDataList.add(emittedTime);
        outputDataList.add(null);
        streamEvent.setOutputData(outputDataList.toArray());

        if (isProcessingExecutor) {
            complexEventChunk.add(streamEvent);
            cudStreamProcessorQueue.add(new QueuedCudStreamProcessor(cudStreamProcessor, streamEvent,
                    startTimeOfNewAggregates, emittedTime, timeZone, duration));
        }
        if (getNextExecutor() != null) {
            next.execute(complexEventChunk);
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

    private long getTimestamp(StreamEvent streamEvent) {
        long timestamp;
        if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
            timestamp = (long) timestampExpressionExecutor.execute(streamEvent);
        } else {
            timestamp = streamEvent.getTimestamp();
        }
        return timestamp;
    }

    @Override
    public Executor getNextExecutor() {
        return next;
    }

    @Override
    public void setNextExecutor(Executor executor) {
        next = executor;
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

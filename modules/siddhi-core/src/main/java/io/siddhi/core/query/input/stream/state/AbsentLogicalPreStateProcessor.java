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

package io.siddhi.core.query.input.stream.state;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.util.Scheduler;
import io.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;
import io.siddhi.query.api.expression.constant.TimeConstant;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Logical not processor.
 */
public class AbsentLogicalPreStateProcessor extends LogicalPreStateProcessor implements AbsentPreStateProcessor {

    /**
     * Scheduler to trigger events after the waitingTime.
     */
    private Scheduler scheduler;

    /**
     * The time defined by 'for' in an absence pattern.
     */
    private long waitingTime = -1;

    public AbsentLogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType,
                                          TimeConstant waitingTime) {
        super(type, stateType);
        if (waitingTime != null) {
            this.waitingTime = waitingTime.value();
        }
    }

    public void init(SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                this.getClass().getName(),
                false, () -> new LogicalStreamPreState());
    }

    @Override
    public void updateLastArrivalTime(long timestamp) {
        StreamPreState state = stateHolder.getState();
        this.lock.lock();
        try {
            ((LogicalStreamPreState) state).lastArrivalTime = timestamp;
        } finally {
            this.lock.unlock();
            stateHolder.returnState(state);
        }
    }

    protected void addState(StateEvent stateEvent, StreamPreState state) {
        if (!((LogicalStreamPreState) state).active) {
            return;
        }
        lock.lock();
        try {
            super.addState(stateEvent, state);
            if (!isStartState) {
                if (waitingTime != -1) {
                    scheduler.notifyAt(stateEvent.getTimestamp() + waitingTime);
                    if (partnerStatePreProcessor instanceof AbsentLogicalPreStateProcessor) {
                        ((AbsentLogicalPreStateProcessor) partnerStatePreProcessor).scheduler.notifyAt(
                                stateEvent.getTimestamp() +
                                        ((AbsentLogicalPreStateProcessor) partnerStatePreProcessor).waitingTime);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {

        StateEvent clonedEvent = stateEventCloner.copyStateEvent(stateEvent);
        clonedEvent.setType(ComplexEvent.Type.CURRENT);
        if (clonedEvent.getStreamEvent(stateId) != null) {
            // Set the timestamp of the last arrived event
            clonedEvent.setTimestamp(clonedEvent.getStreamEvent(stateId).getTimestamp());
        }
        clonedEvent.setEvent(stateId, null);
        clonedEvent.setEvent(partnerStatePreProcessor.stateId, null);
        StreamPreState state = stateHolder.getState();
        try {
            // Start state takes events from newAndEveryStateEventList
            state.getNewAndEveryStateEventList().add(clonedEvent);
            partnerStatePreProcessor.addEventToNewAndEveryStateEventList(clonedEvent);
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {

        StreamPreState state = stateHolder.getState();
        try {
            if (!((LogicalStreamPreState) state).active) {
                return;
            }
            boolean notProcessed = true;
            ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<>();
            this.lock.lock();
            try {
                long currentTime = complexEventChunk.getFirst().getTimestamp();
                if (currentTime >= ((LogicalStreamPreState) state).lastArrivalTime + waitingTime) {
                    Iterator<StateEvent> iterator;
                    if (isStartState && stateType == StateInputStream.Type.SEQUENCE &&
                            state.getNewAndEveryStateEventList().isEmpty() &&
                            state.getPendingStateEventList().isEmpty()) {

                        StateEvent stateEvent = stateEventFactory.newInstance();
                        addState(stateEvent);
                    } else if (stateType == StateInputStream.Type.SEQUENCE &&
                            !state.getNewAndEveryStateEventList().isEmpty()) {
                        this.resetState();
                    }
                    this.updateState();

                    StateEvent expiredStateEvent = null;
                    iterator = state.getPendingStateEventList().iterator();
                    while (iterator.hasNext()) {
                        StateEvent stateEvent = iterator.next();
                        // Remove expired events based on within
                        if (isExpired(stateEvent, currentTime)) {
                            expiredStateEvent = stateEvent;
                            iterator.remove();
                            continue;
                        }
                        // Collect the events that came before the waiting time
                        if (waitingTimePassed(currentTime, stateEvent)) {
                            iterator.remove();
                            if (logicalType == LogicalStateElement.Type.OR && stateEvent.getStreamEvent
                                    (partnerStatePreProcessor.getStateId()) == null) {
                                // OR Partner not received
                                stateEvent.addEvent(stateId, streamEventFactory.newInstance());
                                retEventChunk.add(stateEvent);
                            } else if (logicalType == LogicalStateElement.Type.AND && stateEvent.getStreamEvent
                                    (partnerStatePreProcessor.getStateId()) != null) {
                                // AND partner received but didn't send out
                                retEventChunk.add(stateEvent);
                            } else if (logicalType == LogicalStateElement.Type.AND && stateEvent.getStreamEvent
                                    (partnerStatePreProcessor.getStateId()) == null) {
                                // AND partner didn't receive
                                // Let the partner to process or not
                                stateEvent.addEvent(stateId, streamEventFactory.newInstance());
                            }
                        }
                    }
                    if (expiredStateEvent != null && withinEveryPreStateProcessor != null) {
                        withinEveryPreStateProcessor.addEveryState(expiredStateEvent);
                        withinEveryPreStateProcessor.updateState();
                    }
                    retEventChunk.reset();
                    notProcessed = retEventChunk.getFirst() == null;
                    while (retEventChunk.hasNext()) {
                        StateEvent stateEvent = retEventChunk.next();
                        retEventChunk.remove();
                        stateEvent.setTimestamp(currentTime);
                        sendEvent(stateEvent, state);
                    }
                    ((LogicalStreamPreState) state).lastArrivalTime = 0;
                }
            } finally {
                this.lock.unlock();
            }

            if (thisStatePostProcessor.nextEveryStatePreProcessor != null || (notProcessed && isStartState)) {
                // If every or (notProcessed and startState), schedule again
                long nextBreak;
                if (((LogicalStreamPreState) state).lastArrivalTime == 0) {
                    nextBreak = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime() +
                            waitingTime;
                } else {
                    nextBreak = ((LogicalStreamPreState) state).lastArrivalTime + waitingTime;
                }
                this.scheduler.notifyAt(nextBreak);
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        for (ComplexEventChunk streamEventChunk : complexEventChunks) {
            complexEventChunk.addAll(streamEventChunk);
        }
        process(complexEventChunk);
    }

    private boolean waitingTimePassed(long currentTime, StateEvent event) {
        if (event.getStreamEvent(stateId) == null) {
            // Not processed already
            return currentTime >= event.getTimestamp() + waitingTime;
        } else {
            // Already processed by this processor and added back due to 'every'
            return currentTime >= event.getStreamEvent(stateId).getTimestamp() + waitingTime;
        }
    }

    private void sendEvent(StateEvent stateEvent, StreamPreState state) {
        if (thisStatePostProcessor.nextProcessor != null) {
            thisStatePostProcessor.nextProcessor.process(new ComplexEventChunk<>(stateEvent,
                    stateEvent));
        }
        if (thisStatePostProcessor.nextStatePreProcessor != null) {
            thisStatePostProcessor.nextStatePreProcessor.addState(stateEvent);
        }
        if (thisStatePostProcessor.nextEveryStatePreProcessor != null) {
            thisStatePostProcessor.nextEveryStatePreProcessor.addEveryState(stateEvent);
        } else if (isStartState) {
            ((LogicalStreamPreState) state).active = false;
            if (logicalType == LogicalStateElement.Type.OR &&
                    partnerStatePreProcessor instanceof AbsentLogicalPreStateProcessor) {
                ((AbsentLogicalPreStateProcessor) partnerStatePreProcessor).setActive(false);
            }
        }
        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
            thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
        }
    }

    private void setActive(boolean active) {
        StreamPreState state = stateHolder.getState();
        try {
            ((LogicalStreamPreState) state).active = active;
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {

        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<>();
        StreamPreState state = stateHolder.getState();
        try {

            if (!((LogicalStreamPreState) state).active) {
                return returnEventChunk;
            }
            complexEventChunk.reset();
            StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent
            this.lock.lock();
            try {
                for (Iterator<StateEvent> iterator = state.getPendingStateEventList().iterator();
                     iterator.hasNext(); ) {
                    StateEvent stateEvent = iterator.next();
                    if (logicalType == LogicalStateElement.Type.OR &&
                            stateEvent.getStreamEvent(partnerStatePreProcessor.getStateId()) != null) {
                        iterator.remove();
                        continue;
                    }
                    StreamEvent currentStreamEvent = stateEvent.getStreamEvent(stateId);
                    stateEvent.setEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                    process(stateEvent);
                    if (waitingTime != -1 || (stateType == StateInputStream.Type.SEQUENCE &&
                            logicalType == LogicalStateElement.Type.AND && thisStatePostProcessor
                            .nextEveryStatePreProcessor != null)) {
                        // Reset to the original state after processing
                        stateEvent.setEvent(stateId, currentStreamEvent);
                    }
                    if (this.thisLastProcessor.isEventReturned()) {
                        this.thisLastProcessor.clearProcessedEvent();
                        // The event has passed the filter condition. So remove from being an absent candidate.
                        iterator.remove();
                        if (stateType == StateInputStream.Type.SEQUENCE) {
                            partnerStatePreProcessor.getPendingStateEventList().remove(stateEvent);
                        }
                    }
                    if (!state.isStateChanged()) {
                        switch (stateType) {
                            case PATTERN:
                                stateEvent.setEvent(stateId, currentStreamEvent);
                                break;
                            case SEQUENCE:
                                stateEvent.setEvent(stateId, currentStreamEvent);
                                iterator.remove();
                                break;
                        }
                    }
                }
            } finally {
                this.lock.unlock();
            }
            return returnEventChunk;
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void partitionCreated() {
        StreamPreState state = stateHolder.getState();
        try {
            if (!state.isStarted()) {
                state.started();
                if (isStartState && waitingTime != -1 && ((LogicalStreamPreState) state).active) {
                    this.lock.lock();
                    try {
                        this.scheduler.notifyAt(
                                this.siddhiQueryContext.getSiddhiAppContext().
                                        getTimestampGenerator().currentTime() + waitingTime);
                    } finally {
                        this.lock.unlock();
                    }
                }
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    public boolean partnerCanProceed(StateEvent stateEvent) {
        StreamPreState state = stateHolder.getState();
        try {
            boolean process;
            if (stateType == StateInputStream.Type.SEQUENCE &&
                    thisStatePostProcessor.nextEveryStatePreProcessor == null &&
                    ((LogicalStreamPreState) state).lastArrivalTime > 0) {
                process = false;
            } else {
                if (this.waitingTime == -1) {
                    // for time is not defined and event is not received by absent processor
                    if (thisStatePostProcessor.nextEveryStatePreProcessor == null) {
                        process = stateEvent.getStreamEvent(this.stateId) == null;
                    } else {
                        // Every
                        if (((LogicalStreamPreState) state).lastArrivalTime > 0) {
                            process = false;
                            ((LogicalStreamPreState) state).lastArrivalTime = 0;
                            init();
                        } else {
                            process = true;
                        }
                    }
                } else if (stateEvent.getStreamEvent(this.stateId) != null) {
                    // for time is defined
                    process = true;
                } else {
                    process = false;
                }
            }

            return process;
        } finally {
            stateHolder.returnState(state);
        }
    }

    class LogicalStreamPreState extends StreamPreState {

        /**
         * The timestamp of the last event received by this processor.
         */
        private volatile long lastArrivalTime;

        /**
         * This flag turns to false after processing the first event if 'every' is not used.
         */
        private boolean active = true;

        @Override
        public boolean canDestroy() {
            return super.canDestroy() && lastArrivalTime == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> snapshot = super.snapshot();
            snapshot.put("IsActive", active);
            snapshot.put("LastArrivalTime", lastArrivalTime);
            return snapshot;
        }

        @Override
        public void restore(Map<String, Object> state) {
            super.restore(state);
            active = (Boolean) state.get("IsActive");
            lastArrivalTime = (Long) state.get("LastArrivalTime");
        }
    }
}

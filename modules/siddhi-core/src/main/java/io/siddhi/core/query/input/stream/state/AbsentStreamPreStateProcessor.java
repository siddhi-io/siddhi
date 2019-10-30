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
import io.siddhi.core.util.Scheduler;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Pre processor of not operator.
 */
public class AbsentStreamPreStateProcessor extends StreamPreStateProcessor implements AbsentPreStateProcessor {

    /**
     * Scheduler to trigger events after the waitingTime.
     */
    private Scheduler scheduler;

    /**
     * The time defined by 'for' in an absence pattern.
     */
    private long waitingTime = -1;

    /**
     * Construct an AbsentStreamPreStateProcessor object.
     *
     * @param stateType   PATTERN or SEQUENCE
     * @param waitingTime the waiting time defined by 'for' keyword
     */
    public AbsentStreamPreStateProcessor(StateInputStream.Type stateType,
                                         long waitingTime) {
        super(stateType);
        // Not operator always has 'for' time
        this.waitingTime = waitingTime;
    }

    public void init(SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                this.getClass().getName(),
                false, () -> new LogicalStreamPreState());
    }

    @Override
    public void updateLastArrivalTime(long timestamp) {
        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();
        this.lock.lock();
        try {
            state.lastScheduledTime = timestamp + waitingTime;
            scheduler.notifyAt(state.lastScheduledTime);
        } finally {
            this.lock.unlock();
            stateHolder.returnState(state);
        }
    }

    protected void addState(StateEvent stateEvent, StreamPreState preState) {
        LogicalStreamPreState state = (LogicalStreamPreState) preState;
        if (!state.active) {
            // 'every' keyword is not used and already a pattern is processed
            return;
        }
        lock.lock();
        try {
            if (stateType == StateInputStream.Type.SEQUENCE) {
                state.getNewAndEveryStateEventList().clear();
                state.getNewAndEveryStateEventList().add(stateEvent);
            } else {
                state.getNewAndEveryStateEventList().add(stateEvent);
            }
            // If this is the first processor, nothing to receive from previous patterns
            if (!isStartState) {
                // Start the scheduler
                state.lastScheduledTime = stateEvent.getTimestamp() + waitingTime;
                scheduler.notifyAt(state.lastScheduledTime);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {
        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();
        lock.lock();
        try {
            StateEvent clonedEvent = stateEventCloner.copyStateEvent(stateEvent);
            clonedEvent.setType(ComplexEvent.Type.CURRENT);
            for (int i = stateId; i < clonedEvent.getStreamEvents().length; i++) {
                clonedEvent.setEvent(i, null);
            }
            state.getNewAndEveryStateEventList().add(clonedEvent);
            // Start the scheduler
            state.lastScheduledTime = stateEvent.getTimestamp() + waitingTime;
            scheduler.notifyAt(state.lastScheduledTime);
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public void resetState() {

        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();
        lock.lock();
        try {
            // Clear the events added by the previous processor
            state.getPendingStateEventList().clear();
            if (isStartState) {
                if (stateType == StateInputStream.Type.SEQUENCE &&
                        thisStatePostProcessor.nextEveryStatePreProcessor == null &&
                        !((StreamPreStateProcessor) thisStatePostProcessor.nextStatePreProcessor)
                                .getPendingStateEventList().isEmpty()) {
                    // Sequence without 'every' keyword and the next processor has pending events to be processed
                    return;
                }
                // Start state needs a new event
                init();
            }
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();
        try {
            if (!state.active) {
                // Every keyword is not used and already a pattern is processed
                return;
            }
            boolean notProcessed = true;
            long currentTime = complexEventChunk.getFirst().getTimestamp();
            ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<>();
            lock.lock();
            try {
                // If the process method is called, it is guaranteed that the waitingTime is passed
                boolean initialize = isStartState && state.getNewAndEveryStateEventList().isEmpty()
                        && state.getPendingStateEventList().isEmpty();
                if (initialize && stateType == StateInputStream.Type.SEQUENCE &&
                        thisStatePostProcessor.nextEveryStatePreProcessor == null && state.lastScheduledTime > 0) {
                    // Sequence with no every but an event arrived
                    initialize = false;
                }

                if (initialize) {
                    // This is the first processor and no events received so far
                    StateEvent stateEvent = stateEventFactory.newInstance();
                    addState(stateEvent);
                } else if (stateType == StateInputStream.Type.SEQUENCE &&
                        !state.getNewAndEveryStateEventList().isEmpty()) {
                    this.resetState();
                }
                this.updateState();

                Iterator<StateEvent> iterator = state.getPendingStateEventList().iterator();
                while (iterator.hasNext()) {
                    StateEvent event = iterator.next();
                    // Remove expired events based on within
                    if (isExpired(event, currentTime)) {
                        iterator.remove();
                        if (withinEveryPreStateProcessor != null
                                && thisStatePostProcessor.nextEveryStatePreProcessor != this) {
                            thisStatePostProcessor.nextEveryStatePreProcessor.addEveryState(event);
                        }
                        continue;
                    }
                    // Collect the events that came before the waiting time
                    if (event.getTimestamp() == -1 && currentTime >= state.lastScheduledTime ||
                            event.getTimestamp() != -1 && currentTime >= event.getTimestamp() + waitingTime) {
                        iterator.remove();
                        event.setTimestamp(currentTime);
                        retEventChunk.add(event);
                    }
                }
                if (withinEveryPreStateProcessor != null) {
                    withinEveryPreStateProcessor.updateState();
                }
            } finally {
                lock.unlock();
            }

            notProcessed = retEventChunk.getFirst() == null;
            while (retEventChunk.hasNext()) {
                StateEvent stateEvent = retEventChunk.next();
                retEventChunk.remove();
                sendEvent(stateEvent, state);
            }

            long actualCurrentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            if (actualCurrentTime > waitingTime + currentTime) {
                state.lastScheduledTime = actualCurrentTime + waitingTime;
            }
            if (notProcessed && state.lastScheduledTime < currentTime) {
                state.lastScheduledTime = currentTime + waitingTime;
                this.scheduler.notifyAt(state.lastScheduledTime);
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

    private void sendEvent(StateEvent stateEvent, LogicalStreamPreState state) {
        if (thisStatePostProcessor.nextProcessor != null) {
            thisStatePostProcessor.nextProcessor.process(new ComplexEventChunk<>(stateEvent, stateEvent));
        }
        if (thisStatePostProcessor.nextStatePreProcessor != null) {
            thisStatePostProcessor.nextStatePreProcessor.addState(stateEvent);
        }
        if (thisStatePostProcessor.nextEveryStatePreProcessor != null) {
            thisStatePostProcessor.nextEveryStatePreProcessor.addEveryState(stateEvent);
        } else if (isStartState) {
            state.active = false;
        }

        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
            thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();
        try {
            if (!state.active) {
                return new ComplexEventChunk<>();
            }
            ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);

            StateEvent firstEvent = event.getFirst();
            if (firstEvent != null) {
                event = new ComplexEventChunk<>();
            }
            // Always return an empty event
            return event;
        } finally {
            stateHolder.returnState(state);
        }
    }

    protected boolean removeOnNoStateChange(StateInputStream.Type stateType) {
        return false;
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
        // Start automatically only if it is the start state and 'for' time is defined
        // Otherwise, scheduler will be started in the addState method
        LogicalStreamPreState state = (LogicalStreamPreState) stateHolder.getState();

        try {
            if (!state.isStarted()) {
                state.started();
                if (isStartState && waitingTime != -1 && state.active) {
                    state.lastScheduledTime = this.siddhiQueryContext.getSiddhiAppContext()
                            .getTimestampGenerator().currentTime() + waitingTime;
                    this.scheduler.notifyAt(state.lastScheduledTime);
                }
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    class LogicalStreamPreState extends StreamPreState {

        /**
         * The timestamp of the last event scheduled by this processor.
         */
        private long lastScheduledTime;

        /**
         * This flag turns to false after processing the first event if 'every' is not used.
         * This is used to process only one pattern if 'every' is not used.
         */
        private boolean active = true;

        @Override
        public boolean canDestroy() {
            return super.canDestroy();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> snapshot = super.snapshot();
            snapshot.put("IsActive", active);
            snapshot.put("LastScheduledTime", lastScheduledTime);
            return snapshot;
        }

        @Override
        public void restore(Map<String, Object> state) {
            super.restore(state);
            active = (Boolean) state.get("IsActive");
            lastScheduledTime = (Long) state.get("LastScheduledTime");
        }
    }
}

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

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.query.input.stream.single.EntryValveProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.parser.SchedulerParser;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;

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
     * The timestamp of the last event scheduled by this processor.
     */
    private long lastScheduledTime;

    /**
     * This flag turns to false after processing the first event if 'every' is not used.
     * This is used to process only one pattern if 'every' is not used.
     */
    private boolean active = true;

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

    @Override
    public void updateLastArrivalTime(long timestamp) {
        synchronized (this) {
            this.lastScheduledTime = timestamp + waitingTime;
            scheduler.notifyAt(lastScheduledTime);
        }
    }

    @Override
    public void addState(StateEvent stateEvent) {

        if (!this.active) {
            // 'every' keyword is not used and already a pattern is processed
            return;
        }
        lock.lock();
        try {
            if (stateType == StateInputStream.Type.SEQUENCE) {
                newAndEveryStateEventList.clear();
                newAndEveryStateEventList.add(stateEvent);
            } else {
                newAndEveryStateEventList.add(stateEvent);
            }
            // If this is the first processor, nothing to receive from previous patterns
            if (!isStartState) {
                // Start the scheduler
                lastScheduledTime = stateEvent.getTimestamp() + waitingTime;
                scheduler.notifyAt(lastScheduledTime);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {
        lock.lock();
        try {
            StateEvent clonedEvent = stateEventCloner.copyStateEvent(stateEvent);
            newAndEveryStateEventList.add(clonedEvent);
            // Start the scheduler
            lastScheduledTime = stateEvent.getTimestamp() + waitingTime;
            scheduler.notifyAt(lastScheduledTime);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resetState() {

        lock.lock();
        try {
            // Clear the events added by the previous processor
            pendingStateEventList.clear();
            if (isStartState) {
                if (stateType == StateInputStream.Type.SEQUENCE &&
                        thisStatePostProcessor.nextEveryStatePreProcessor == null &&
                        !((StreamPreStateProcessor) thisStatePostProcessor.nextStatePreProcessor)
                                .pendingStateEventList.isEmpty()) {
                    // Sequence without 'every' keyword and the next processor has pending events to be processed
                    return;
                }
                // Start state needs a new event
                init();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (!this.active) {
            // Every keyword is not used and already a pattern is processed
            return;
        }
        boolean notProcessed = true;
        long currentTime = complexEventChunk.getFirst().getTimestamp();
        ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<>(false);
        lock.lock();
        try {
            // If the process method is called, it is guaranteed that the waitingTime is passed
            boolean initialize = isStartState && newAndEveryStateEventList.isEmpty()
                    && pendingStateEventList.isEmpty();
            if (initialize && stateType == StateInputStream.Type.SEQUENCE &&
                    thisStatePostProcessor.nextEveryStatePreProcessor == null && this.lastScheduledTime > 0) {
                // Sequence with no every but an event arrived
                initialize = false;
            }

            if (initialize) {
                // This is the first processor and no events received so far
                StateEvent stateEvent = stateEventPool.borrowEvent();
                addState(stateEvent);
            } else if (stateType == StateInputStream.Type.SEQUENCE && !newAndEveryStateEventList.isEmpty()) {
                this.resetState();
            }
            this.updateState();

            Iterator<StateEvent> iterator = pendingStateEventList.iterator();
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
                if (event.getTimestamp() == -1 && currentTime >= lastScheduledTime ||
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
            sendEvent(stateEvent);
        }

        long actualCurrentTime = siddhiAppContext.getTimestampGenerator().currentTime();
        if (actualCurrentTime > waitingTime + currentTime) {
            lastScheduledTime = actualCurrentTime + waitingTime;
        }
        if (notProcessed && lastScheduledTime < currentTime) {
            lastScheduledTime = currentTime + waitingTime;
            this.scheduler.notifyAt(lastScheduledTime);
        }
    }

    private void sendEvent(StateEvent stateEvent) {
        if (thisStatePostProcessor.nextProcessor != null) {
            thisStatePostProcessor.nextProcessor.process(new ComplexEventChunk<>(stateEvent, stateEvent, false));
        }
        if (thisStatePostProcessor.nextStatePreProcessor != null) {
            thisStatePostProcessor.nextStatePreProcessor.addState(stateEvent);
        }
        if (thisStatePostProcessor.nextEveryStatePreProcessor != null) {
            thisStatePostProcessor.nextEveryStatePreProcessor.addEveryState(stateEvent);
        } else if (isStartState) {
            this.active = false;
        }

        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
            thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {

        if (!this.active) {
            return new ComplexEventChunk<>(false);
        }
        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);

        StateEvent firstEvent = event.getFirst();
        if (firstEvent != null) {
            event = new ComplexEventChunk<>(false);
        }
        // Always return an empty event
        return event;
    }

    protected boolean removeOnNoStateChange(StateInputStream.Type stateType) {
        return false;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public PreStateProcessor cloneProcessor(String key) {
        AbsentStreamPreStateProcessor streamPreStateProcessor = new AbsentStreamPreStateProcessor(stateType,
                waitingTime);
        cloneProperties(streamPreStateProcessor, key);
        streamPreStateProcessor.init(siddhiAppContext, queryName);

        // Set the scheduler
        siddhiAppContext.addEternalReferencedHolder(streamPreStateProcessor);
        EntryValveProcessor entryValveProcessor = new EntryValveProcessor(siddhiAppContext);
        entryValveProcessor.setToLast(streamPreStateProcessor);
        Scheduler scheduler = SchedulerParser.parse(entryValveProcessor, siddhiAppContext);
        streamPreStateProcessor.setScheduler(scheduler);
        return streamPreStateProcessor;
    }

    @Override
    public void start() {
        // Start automatically only if it is the start state and 'for' time is defined
        // Otherwise, scheduler will be started in the addState method
        if (isStartState && waitingTime != -1 && active) {
            synchronized (this) {
                lastScheduledTime = this.siddhiAppContext.getTimestampGenerator().currentTime() + waitingTime;
                this.scheduler.notifyAt(lastScheduledTime);
            }
        }
    }

    @Override
    public void stop() {
        // Scheduler will be stopped automatically
        // Nothing to stop here
    }
}

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
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.expression.constant.TimeConstant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre processor of not operator.
 */
public class AbsentStreamPreStateProcessor extends StreamPreStateProcessor implements AbsentPreStateProcessor {

    private Scheduler scheduler;
    private List<StateEvent> arrivedEventsList = new LinkedList<>();
    private long waitingTime;
    private boolean noPresentBefore = false;
    private volatile long lastArrivalTime;


    public AbsentStreamPreStateProcessor(StateInputStream.Type stateType, List<Map.Entry<Long, Set<Integer>>>
            withinStates, TimeConstant waitingTime) {
        super(stateType, withinStates);
        // Not operator always has within time
        this.waitingTime = waitingTime.value();
    }

    public void updateLastArrivalTime(long timestamp) {
        synchronized (this) {
            this.lastArrivalTime = timestamp;
        }
    }

    @Override
    public void setNoPresentBefore(boolean noPresentBeforeInPattern) {
        this.noPresentBefore = noPresentBeforeInPattern;
    }

    @Override
    public boolean isNoPresentBefore() {
        return this.noPresentBefore;
    }

    @Override
    public long getWaitingTime() {
        return this.waitingTime;
    }

    @Override
    public void addState(StateEvent stateEvent) {
        super.addState(stateEvent);
        if (!isStartState) {
            if (isNextToAbsentProcessor()) {
                scheduler.notifyAt(stateEvent.getTimestamp() + absentPreStateProcessor.getWaitingTime());
            } else {
                synchronized (this) {
                    arrivedEventsList.add(stateEvent);
                }
                scheduler.notifyAt(stateEvent.getTimestamp() + waitingTime);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        boolean empty;
        if (isStartState) {
            empty = true;
        } else {
            synchronized (this) {
                empty = noPresentBefore && this.newAndEveryStateEventList.isEmpty();
            }
        }
        return empty;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {

        boolean notProcessed = true;
        long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
        if (currentTime >= this.lastArrivalTime + waitingTime) {
            synchronized (this) {
                // If the process method is called, it is guaranteed that the waitingTime is passed
                boolean empty;
                empty = isStartState && noPresentBefore && this.arrivedEventsList.isEmpty();
                if (empty) {

                    // This is the first processor and no events received so far
                    StateEvent stateEvent = stateEventPool.borrowEvent();
                    stateEvent.setTimestamp(currentTime);
                    sendEvent(stateEvent);
                    notProcessed = false;

                } else {
                    // This processor is next to some other processors
                    ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<>(false);

                    Iterator<StateEvent> iterator = arrivedEventsList.iterator();
                    while (iterator.hasNext()) {
                        StateEvent event = iterator.next();
                        // Remove expired events based on within
                        if (withinStates.size() > 0) {
                            if (isExpired(event, currentTime)) {
                                iterator.remove();
                                continue;
                            }
                        }
                        // Collect the events that came before the waiting time
                        if (currentTime >= event.getTimestamp() + waitingTime) {
                            if (thisStatePostProcessor.nextEveryStatePerProcessor == null) {
                                iterator.remove();
                            } else {
                                // Every
                                event = stateEventCloner.copyStateEvent(event);
                            }
                            event.setTimestamp(currentTime);
                            retEventChunk.add(event);
                        }
                    }

                    notProcessed = retEventChunk.getFirst() == null;
                    while (retEventChunk.hasNext()) {
                        StateEvent stateEvent = retEventChunk.next();
                        retEventChunk.remove();
                        sendEvent(stateEvent);
                    }

                }
            }
            this.lastArrivalTime = 0;
        }
        if (thisStatePostProcessor.nextEveryStatePerProcessor == this || (notProcessed && isStartState)) {
            long nextBreak;
            if (lastArrivalTime == 0) {
                nextBreak = currentTime + waitingTime;
            } else {
                nextBreak = lastArrivalTime + waitingTime;
            }
            this.scheduler.notifyAt(nextBreak);
        }

    }

    private void sendEvent(StateEvent stateEvent) {
        if (thisStatePostProcessor.nextProcessor != null) {
            thisStatePostProcessor.nextProcessor.process(new ComplexEventChunk<>(stateEvent,
                    stateEvent, false));
        }
        if (thisStatePostProcessor.nextStatePerProcessor != null) {
            thisStatePostProcessor.nextStatePerProcessor.addState(stateEvent);
        }
        if (thisStatePostProcessor.nextEveryStatePerProcessor != null) {
            thisStatePostProcessor.nextEveryStatePerProcessor.addEveryState(stateEvent);
        }
        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
            thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
        }
    }

    private boolean isExpired(StateEvent pendingStateEvent, long currentTime) {
        for (Map.Entry<Long, Set<Integer>> withinEntry : withinStates) {
            for (Integer withinStateId : withinEntry.getValue()) {
                if (withinStateId == SiddhiConstants.ANY) {
                    if (Math.abs(pendingStateEvent.getTimestamp() - currentTime) > withinEntry.getKey()) {
                        return true;
                    }
                } else {
                    if (Math.abs(pendingStateEvent.getStreamEvent(withinStateId).getTimestamp() - currentTime) >
                            withinEntry.getKey()) {
                        return true;

                    }
                }
            }
        }
        return false;
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        if (!isStartState) {
            StateEvent firstEvent = event.getFirst();
            if (firstEvent != null) {
                // Synchronize with process method
                synchronized (this) {
                    arrivedEventsList.remove(firstEvent);
                }
                event = new ComplexEventChunk<>(false);
            }
        }
        return event;
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
    public void start() {
        if (isStartState && waitingTime != -1) {
            synchronized (this) {
                if (this.arrivedEventsList.isEmpty()) {
                    this.scheduler.notifyAt(this.executionPlanContext.getTimestampGenerator().currentTime() +
                            waitingTime);
                }
            }
        }
    }

    @Override
    public void stop() {
        // Scheduler will be stopped automatically
        // Nothing to stop here
    }
}

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
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.expression.constant.TimeConstant;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Logical not processor.
 */
public class AbsentLogicalPreStateProcessor extends LogicalPreStateProcessor implements AbsentPreStateProcessor {

    private Scheduler scheduler;
    private List<StateEvent> arrivedEventsList = new LinkedList<>();
    private long waitingTime = -1;
    private boolean noPresentBefore;

    public AbsentLogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType,
                                          List<Map.Entry<Long, Set<Integer>>> withinStates, TimeConstant waitingTime) {
        super(type, stateType, withinStates);
        if (waitingTime != null) {
            this.waitingTime = waitingTime.value();
        }
    }

    @Override
    public void addState(StateEvent stateEvent) {
        super.addState(stateEvent);
        if (!isStartState) {
            if (waitingTime != -1) {
                synchronized (this) {
                    arrivedEventsList.add(stateEvent);
                }
                scheduler.notifyAt(stateEvent.getTimestamp() + waitingTime);
            }
        }
    }

    @Override
    public long getWaitingTime() {
        return waitingTime;
    }

    @Override
    public boolean isEmpty() {
        boolean empty;
        if (isStartState) {
            empty = true;
        } else {
            synchronized (this) {
                empty = noPresentBefore && logicalType == LogicalStateElement.Type.OR &&
                        this.newAndEveryStateEventList.isEmpty();
            }
        }
        return empty;
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
    public void process(ComplexEventChunk complexEventChunk) {

        // If the process method is called, it is guaranteed that the waitingTime is passed
        long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
        ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<>(false);

        synchronized (this) {
            Iterator<StateEvent> iterator;
            if (isStartState) {
                iterator = pendingStateEventList.iterator();
                if (pendingStateEventList.isEmpty() && logicalType == LogicalStateElement.Type.OR) {
                    retEventChunk.add(stateEventPool.borrowEvent());
                }
                if (pendingStateEventList.isEmpty() && logicalType == LogicalStateElement.Type.AND) {
                    if (!partnerStatePreProcessor.pendingStateEventList.isEmpty()) {
                        partnerStatePreProcessor.pendingStateEventList.get(0).addEvent(stateId, streamEventPool
                                .borrowEvent());
                    } else if (!partnerStatePreProcessor.newAndEveryStateEventList.isEmpty()) {
                        partnerStatePreProcessor.newAndEveryStateEventList.get(0).addEvent(stateId, streamEventPool
                                .borrowEvent());
                    }
                }
            } else {
                iterator = arrivedEventsList.iterator();
            }

            while (iterator.hasNext()) {
                StateEvent stateEvent = iterator.next();
                if (currentTime >= stateEvent.getTimestamp() + waitingTime) {
                    iterator.remove();

                    if (logicalType == LogicalStateElement.Type.OR && stateEvent.getStreamEvent
                            (partnerStatePreProcessor.getStateId()) == null) {
                        // OR Partner not received
                        retEventChunk.add(stateEvent);
                    } else if (logicalType == LogicalStateElement.Type.AND && stateEvent.getStreamEvent
                            (partnerStatePreProcessor.getStateId()) != null) {
                        // AND partner received but didn't send out
                        retEventChunk.add(stateEvent);
                    } else if (logicalType == LogicalStateElement.Type.AND && stateEvent.getStreamEvent
                            (partnerStatePreProcessor.getStateId()) == null) {
                        // AND partner didn't receive
                        // Let the partner to process or not
                        stateEvent.addEvent(stateId, streamEventPool.borrowEvent());
                    }
                }
            }
        }

        retEventChunk.reset();
        while (retEventChunk.hasNext()) {
            StateEvent stateEvent = retEventChunk.next();
            retEventChunk.remove();
            sendEvent(stateEvent);
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

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {

        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        if (logicalType == LogicalStateElement.Type.OR ||
                (logicalType == LogicalStateElement.Type.AND && waitingTime != -1)) {


            StateEvent firstEvent = event.getFirst();
            if (firstEvent != null) {
                while (event.hasNext()) {
                    firstEvent = event.next();
                    // Synchronize with process method
                    synchronized (this) {
                        arrivedEventsList.remove(firstEvent);
                    }
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

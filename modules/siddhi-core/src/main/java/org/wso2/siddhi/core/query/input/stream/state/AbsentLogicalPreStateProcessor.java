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

import org.wso2.siddhi.core.event.ComplexEvent;
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
    private boolean waitingTimePassed = false;
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
            if (logicalType == LogicalStateElement.Type.OR ||
                    (logicalType == LogicalStateElement.Type.AND && waitingTime != -1)) {
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
    public boolean isWaitingTimePassed() {
        return waitingTimePassed;
    }

    @Override
    public void setNoPresentBefore(boolean noPresentBeforeInPattern) {
        this.noPresentBefore = noPresentBeforeInPattern;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {

        // If the process method is called, it is guaranteed that the waitingTime is passed
        waitingTimePassed = true;

        if (logicalType == LogicalStateElement.Type.OR ||
                (logicalType == LogicalStateElement.Type.AND && waitingTime != -1)) {

            while (complexEventChunk.hasNext()) {
                ComplexEvent newEvent = complexEventChunk.next();
                if (newEvent.getType() == ComplexEvent.Type.TIMER) {
                    long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                    ComplexEventChunk<StateEvent> retEventChunk = new ComplexEventChunk<StateEvent>(false);

                    // Synchronize with processAndReturn method
                    synchronized (this) {
                        Iterator<StateEvent> iterator = arrivedEventsList.iterator();
                        while (iterator.hasNext()) {
                            StateEvent event = iterator.next();
                            if (currentTime >= event.getTimestamp() + waitingTime) {
                                iterator.remove();
                                retEventChunk.add(event);
                            }
                        }
                    }

                    while (retEventChunk.hasNext()) {
                        StateEvent stateEvent = retEventChunk.next();
                        retEventChunk.remove();
                        if (thisStatePostProcessor.nextProcessor != null &&
                                logicalType == LogicalStateElement.Type.OR) {
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
                }
            }
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {

        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        if (logicalType == LogicalStateElement.Type.OR ||
                (logicalType == LogicalStateElement.Type.AND && waitingTime != -1)) {
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
        this.scheduler.notifyAt(this.executionPlanContext.getTimestampGenerator().currentTime() + waitingTime);
    }

    @Override
    public void stop() {
        // Scheduler will be stopped automatically
        // Nothing to stop here
    }
}

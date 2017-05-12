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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre processor of not operator.
 */
public class AbsentStreamPreStateProcessor extends StreamPreStateProcessor implements SchedulingProcessor {

    private Scheduler scheduler;
    private List<StateEvent> arrivedEventsList = new LinkedList<>();
    private long timeout;
    private boolean isFirstInPattern = false;
    private boolean noPresentBeforeInPattern = false;


    public AbsentStreamPreStateProcessor(StateInputStream.Type stateType, List<Map.Entry<Long, Set<Integer>>>
            withinStates) {
        super(stateType, Collections.EMPTY_LIST);
        // TODO: 4/9/17 Make sure that this implementation is correct
        timeout = withinStates.get(0).getKey();
    }

    public boolean isFirstInPattern() {
        return isFirstInPattern;
    }

    public void setFirstInPattern(boolean firstInPattern) {
        isFirstInPattern = firstInPattern;
    }

    public boolean isNoPresentBeforeInPattern() {
        return noPresentBeforeInPattern;
    }

    public void setNoPresentBeforeInPattern(boolean noPresentBeforeInPattern) {
        this.noPresentBeforeInPattern = noPresentBeforeInPattern;
    }

    public long getTimeout() {
        return this.timeout;
    }

    @Override
    public void addState(StateEvent stateEvent) {
        super.addState(stateEvent);
        if (!isFirstInPattern) {
            if (isAbsentPartner()) {
                scheduler.notifyAt(stateEvent.getTimestamp() + getAbsentPartnerTimeout());
            } else {
                synchronized (this) {
                    arrivedEventsList.add(stateEvent);
                }
                scheduler.notifyAt(stateEvent.getTimestamp() + timeout);
            }

        }
    }

    public boolean hasStateEvents() {
        boolean hasState;
        if (isFirstInPattern) {
            hasState = false;
        } else if (isAbsentPartner()) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            for (Iterator<StateEvent> iterator = newAndEveryStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (currentTime >= stateEvent.getTimestamp() + getAbsentPartnerTimeout()) {
                    iterator.remove();
                }
            }
            hasState = !pendingStateEventList.isEmpty() || !this.newAndEveryStateEventList.isEmpty() ||
                    getAbsentStreamPreStateProcessor().hasStateEvents();
        } else {
            hasState = !this.newAndEveryStateEventList.isEmpty();
        }
        return hasState;

    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {

        if (isFirstInPattern) {
            super.process(complexEventChunk);
        } else {
            // Called by the scheduler

            if (isAbsentPartner()) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent newEvent = complexEventChunk.next();
                    if (newEvent.getType() == ComplexEvent.Type.TIMER) {
                        long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                        for (Iterator<StateEvent> iterator = pendingStateEventList.iterator(); iterator.hasNext(); ) {
                            if (currentTime >= iterator.next().getTimestamp() + getAbsentPartnerTimeout()) {
                                iterator.remove();
                            }
                        }
                    }
                }
            } else {
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
                                if (currentTime >= event.getTimestamp() + timeout) {
                                    iterator.remove();
                                    retEventChunk.add(event);
                                }
                            }
                        }

                        if (this.getThisStatePostProcessor().getNextProcessor() != null) {
                            // Next processor is StreamStatePostProcessor
                            QuerySelector querySelector = (QuerySelector) this.getThisStatePostProcessor()
                                    .getNextProcessor();
                            while (retEventChunk.hasNext()) {
                                StateEvent stateEvent = retEventChunk.next();
                                retEventChunk.remove();
                                querySelector.process(new ComplexEventChunk<StateEvent>(stateEvent, stateEvent, false));
                            }
                        } else if (this.getThisStatePostProcessor().getNextStatePerProcessor() != null) {
                            // No next StreamStatePostProcessor, if this is part of pattern
                            while (retEventChunk.hasNext()) {
                                StateEvent stateEvent = retEventChunk.next();
                                retEventChunk.remove();
                                // TODO: 5/1/17 Does it casue to any other issues?
                                ((StreamPreStateProcessor) this.getThisStatePostProcessor().getNextStatePerProcessor
                                        ()).addAbsent(stateEvent);

                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        if (!isFirstInPattern) {
            StateEvent firstEvent = event.getFirst();
            if (firstEvent != null) {
                // Synchronize with process method
                synchronized (this) {
                    arrivedEventsList.remove(firstEvent);
                }
                event = new ComplexEventChunk<StateEvent>(false);
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
}

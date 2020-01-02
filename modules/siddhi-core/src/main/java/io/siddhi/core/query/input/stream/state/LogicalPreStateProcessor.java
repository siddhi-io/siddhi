/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;

/**
 * Logical and &amp; or processor.
 */
public class LogicalPreStateProcessor extends StreamPreStateProcessor {

    protected LogicalStateElement.Type logicalType;
    protected LogicalPreStateProcessor partnerStatePreProcessor;

    public LogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType) {
        super(stateType);
        this.logicalType = type;
    }

    protected void addState(StateEvent stateEvent, StreamPreState state) {
        lock.lock();
        try {
            if (isStartState || stateType == StateInputStream.Type.SEQUENCE) {
                if (state.getNewAndEveryStateEventList().isEmpty()) {
                    state.getNewAndEveryStateEventList().add(stateEvent);
                }
                if (partnerStatePreProcessor != null && partnerStatePreProcessor.isNewAndEveryStateEventListEmpty()) {
                    partnerStatePreProcessor.addEventToNewAndEveryStateEventList(stateEvent);
                }
            } else {
                state.getNewAndEveryStateEventList().add(stateEvent);
                if (partnerStatePreProcessor != null) {
                    partnerStatePreProcessor.addEventToNewAndEveryStateEventList(stateEvent);
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
        clonedEvent.setEvent(stateId, null);
        for (int i = stateId; i < clonedEvent.getStreamEvents().length; i++) {
            clonedEvent.setEvent(i, null);
        }
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            state.getNewAndEveryStateEventList().add(clonedEvent);
            if (partnerStatePreProcessor != null) {
                clonedEvent.setEvent(partnerStatePreProcessor.stateId, null);
                partnerStatePreProcessor.addEventToNewAndEveryStateEventList(clonedEvent);
            }
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public void resetState() {
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            if (logicalType == LogicalStateElement.Type.OR || state.getPendingStateEventList().size() ==
                    partnerStatePreProcessor.getPendingStateEventList().size()) {
                state.getPendingStateEventList().clear();
                partnerStatePreProcessor.getPendingStateEventList().clear();

                if (isStartState && state.getNewAndEveryStateEventList().isEmpty()) {
                    if (stateType == StateInputStream.Type.SEQUENCE &&
                            thisStatePostProcessor.nextEveryStatePreProcessor == null &&
                            !((StreamPreStateProcessor) thisStatePostProcessor.nextStatePreProcessor)
                                    .getPendingStateEventList().isEmpty()) {
                        return;
                    }
                    init();
                }
            }
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public void updateState() {
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            state.getNewAndEveryStateEventList().sort(eventTimeComparator);
            state.getPendingStateEventList().addAll(state.getNewAndEveryStateEventList());
            state.getNewAndEveryStateEventList().clear();
            partnerStatePreProcessor.moveAllNewAndEveryStateEventListEventsToPendingStateEventList();
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<StateEvent>();
        complexEventChunk.reset();
        StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            for (Iterator<StateEvent> iterator = state.getPendingStateEventList().iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (logicalType == LogicalStateElement.Type.OR &&
                        stateEvent.getStreamEvent(partnerStatePreProcessor.getStateId()) != null) {
                    iterator.remove();
                    continue;
                }
                stateEvent.setEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                process(stateEvent);
                if (this.thisLastProcessor.isEventReturned()) {
                    this.thisLastProcessor.clearProcessedEvent();
                    returnEventChunk.add(stateEvent);
                }
                if (state.isStateChanged()) {
                    iterator.remove();
                } else {
                    switch (stateType) {
                        case PATTERN:
                            stateEvent.setEvent(stateId, null);
                            break;
                        case SEQUENCE:
                            stateEvent.setEvent(stateId, null);
                            iterator.remove();
                            break;
                    }
                }
            }
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
        return returnEventChunk;
    }

    public void setPartnerStatePreProcessor(LogicalPreStateProcessor partnerStatePreProcessor) {
        this.partnerStatePreProcessor = partnerStatePreProcessor;
        partnerStatePreProcessor.lock = lock;
    }

    public void moveAllNewAndEveryStateEventListEventsToPendingStateEventList() {
        StreamPreState state = stateHolder.getState();
        try {
            state.getNewAndEveryStateEventList().sort(eventTimeComparator);
            state.getPendingStateEventList().addAll(state.getNewAndEveryStateEventList());
            state.getNewAndEveryStateEventList().clear();
        } finally {
            stateHolder.returnState(state);
        }
    }

    public boolean isNewAndEveryStateEventListEmpty() {
        StreamPreState state = stateHolder.getState();
        try {
            return state.getNewAndEveryStateEventList().isEmpty();
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void addEventToNewAndEveryStateEventList(StateEvent event) {
        StreamPreState state = stateHolder.getState();
        try {
            state.getNewAndEveryStateEventList().add(event);
        } finally {
            stateHolder.returnState(state);
        }
    }
}

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

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;

/**
 * Logical and &amp; or processor.
 */
public class LogicalPreStateProcessor extends StreamPreStateProcessor {

    protected LogicalStateElement.Type logicalType;
    protected LogicalPreStateProcessor partnerStatePreProcessor;

    public LogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType,
                                    Long withinTime) {
        super(stateType, withinTime);
        this.logicalType = type;
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of LogicalPreStateProcessor
     */
    @Override
    public PreStateProcessor cloneProcessor(String key) {
        LogicalPreStateProcessor logicalPreStateProcessor = new LogicalPreStateProcessor(logicalType, stateType,
                withinTime);
        cloneProperties(logicalPreStateProcessor, key);
        logicalPreStateProcessor.init(siddhiAppContext, queryName);
        return logicalPreStateProcessor;
    }

    @Override
    public void addState(StateEvent stateEvent) {
        lock.lock();
        try {
            if (isStartState || stateType == StateInputStream.Type.SEQUENCE) {
                if (newAndEveryStateEventList.isEmpty()) {
                    newAndEveryStateEventList.add(stateEvent);
                }
                if (partnerStatePreProcessor != null && partnerStatePreProcessor.newAndEveryStateEventList.isEmpty()) {
                    partnerStatePreProcessor.newAndEveryStateEventList.add(stateEvent);
                }
            } else {
                newAndEveryStateEventList.add(stateEvent);
                if (partnerStatePreProcessor != null) {
                    partnerStatePreProcessor.newAndEveryStateEventList.add(stateEvent);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {
        StateEvent clonedEvent = stateEventCloner.copyStateEvent(stateEvent);
        // Set the timestamp of the last arrived event
        if (clonedEvent.getStreamEvent(stateId) != null) {
            clonedEvent.setTimestamp(clonedEvent.getStreamEvent(stateId).getTimestamp());
        } else if(clonedEvent.getStreamEvent(partnerStatePreProcessor.stateId) != null) {
            clonedEvent.setTimestamp(clonedEvent.getStreamEvent(partnerStatePreProcessor.stateId).getTimestamp());
        }
        clonedEvent.setEvent(stateId, null);
        lock.lock();
        try {
            newAndEveryStateEventList.add(clonedEvent);
            if (partnerStatePreProcessor != null) {
                clonedEvent.setEvent(partnerStatePreProcessor.stateId, null);
                partnerStatePreProcessor.newAndEveryStateEventList.add(clonedEvent);
            }
        } finally {
            lock.unlock();
        }
    }

    public void setStartState(boolean isStartState) {
        this.isStartState = isStartState;
        this.getThisStatePostProcessor().setStartState(isStartState);
        if (partnerStatePreProcessor.isStartState != this.isStartState) {
            partnerStatePreProcessor.isStartState = isStartState;
            partnerStatePreProcessor.getThisStatePostProcessor().setStartState(isStartState);
        }
    }

    @Override
    public void resetState() {
        lock.lock();
        try {
            if (logicalType == LogicalStateElement.Type.OR || pendingStateEventList.size() ==
                    partnerStatePreProcessor.pendingStateEventList.size()) {
                pendingStateEventList.clear();
                partnerStatePreProcessor.pendingStateEventList.clear();

                if (isStartState && newAndEveryStateEventList.isEmpty()) {
                    if (stateType == StateInputStream.Type.SEQUENCE &&
                            thisStatePostProcessor.nextEveryStatePreProcessor == null &&
                            !((StreamPreStateProcessor) thisStatePostProcessor.nextStatePreProcessor)
                                    .pendingStateEventList.isEmpty()) {
                        return;
                    }
                    init();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void updateState() {
        lock.lock();
        try {
            pendingStateEventList.addAll(newAndEveryStateEventList);
            newAndEveryStateEventList.clear();

            partnerStatePreProcessor.pendingStateEventList.addAll(partnerStatePreProcessor.newAndEveryStateEventList);
            partnerStatePreProcessor.newAndEveryStateEventList.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<StateEvent>(false);
        complexEventChunk.reset();
        StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent
        lock.lock();
        try {
            for (Iterator<StateEvent> iterator = pendingStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (isExpired(stateEvent, streamEvent.getTimestamp())) {
                    if (withinEveryPreStateProcessor != null) {
                        withinEveryPreStateProcessor.addEveryState(stateEvent);
                        withinEveryPreStateProcessor.updateState();
                    }
                    iterator.remove();
                    continue;
                }
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
                if (stateChanged) {
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
        }
        return returnEventChunk;
    }

    public void setPartnerStatePreProcessor(LogicalPreStateProcessor partnerStatePreProcessor) {
        this.partnerStatePreProcessor = partnerStatePreProcessor;
        partnerStatePreProcessor.lock = lock;
    }
}

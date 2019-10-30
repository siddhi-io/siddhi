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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;
import java.util.Map;

/**
 * Created on 1/6/15.
 */
public class CountPreStateProcessor extends StreamPreStateProcessor {
    private final int minCount;
    private final int maxCount;
    private CountPostStateProcessor countPostStateProcessor;

    public CountPreStateProcessor(int minCount, int maxCount, StateInputStream.Type stateType) {
        super(stateType);
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    public void init(SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                this.getClass().getName(),
                false, () -> new CountStreamPreState());
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<StateEvent>();
        complexEventChunk.reset();
        StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent
        CountStreamPreState state = (CountStreamPreState) stateHolder.getState();
        lock.lock();
        try {
            for (Iterator<StateEvent> iterator = state.getPendingStateEventList().iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (removeIfNextStateProcessed(stateEvent, iterator, stateId + 1)) {
                    continue;
                }
                if (removeIfNextStateProcessed(stateEvent, iterator, stateId + 2)) {
                    continue;
                }
                stateEvent.addEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                state.successCondition = false;
                process(stateEvent);
                if (this.thisLastProcessor.isEventReturned()) {
                    this.thisLastProcessor.clearProcessedEvent();
                    returnEventChunk.add(stateEvent);
                }
                if (state.isStateChanged()) {
                    iterator.remove();
                }
                if (!state.successCondition) {
                    switch (stateType) {
                        case PATTERN:
                            stateEvent.removeLastEvent(stateId);
                            break;
                        case SEQUENCE:
                            stateEvent.removeLastEvent(stateId);
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

    private boolean removeIfNextStateProcessed(StateEvent stateEvent, Iterator<StateEvent> iterator, int position) {
        if (stateEvent.getStreamEvents().length > position && stateEvent.getStreamEvent(position) != null) {
            iterator.remove();
            return true;
        }
        return false;
    }

    public void successCondition() {
        CountStreamPreState state = (CountStreamPreState) stateHolder.getState();
        try {
            state.successCondition = true;
        } finally {
            stateHolder.returnState(state);
        }
    }

    protected void addState(StateEvent stateEvent, StreamPreState state) {
        //        if (stateType == StateInputStream.Type.SEQUENCE) {
        //            newAndEveryStateEventList.clear();
        //            pendingStateEventList.clear();
        //        }
        lock.lock();
        try {
            if (stateType == StateInputStream.Type.SEQUENCE) {
                if (state.getNewAndEveryStateEventList().isEmpty()) {
                    state.getNewAndEveryStateEventList().add(stateEvent);
                }
            } else {
                state.getNewAndEveryStateEventList().add(stateEvent);
            }
        } finally {
            lock.unlock();
        }
        if (minCount == 0 && stateEvent.getStreamEvent(stateId) == null) {
            ComplexEventChunk<StateEvent> eventChunk = state.getCurrentStateEventChunk();
            eventChunk.clear();
            eventChunk.add(stateEvent);
            countPostStateProcessor.processMinCountReached(stateEvent, eventChunk);
            eventChunk.clear();
        }
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {
        lock.lock();
        try {
            StateEvent clonedEvent = stateEventCloner.copyStateEvent(stateEvent);
            clonedEvent.setType(ComplexEvent.Type.CURRENT);
            for (int i = stateId; i < clonedEvent.getStreamEvents().length; i++) {
                clonedEvent.setEvent(i, null);
            }
            StreamPreState state = stateHolder.getState();
            try {
                state.getNewAndEveryStateEventList().add(clonedEvent);
            } finally {
                stateHolder.returnState(state);
            }
        } finally {
            lock.unlock();
        }
    }

    public CountPostStateProcessor getCountPostStateProcessor() {
        return countPostStateProcessor;
    }

    public void setCountPostStateProcessor(CountPostStateProcessor countPostStateProcessor) {
        this.countPostStateProcessor = countPostStateProcessor;
    }

    public void startStateReset() {
        CountStreamPreState state = (CountStreamPreState) stateHolder.getState();
        try {
            state.startStateReset = true;
            if (thisStatePostProcessor.callbackPreStateProcessor != null) {
                ((CountPreStateProcessor) countPostStateProcessor.thisStatePreProcessor).startStateReset();
            }
        } finally {
            stateHolder.returnState(state);
        }

    }

    @Override
    public void updateState() {
        CountStreamPreState state = (CountStreamPreState) stateHolder.getState();
        try {
            if (state.startStateReset) {
                state.startStateReset = false;
                init();
            }
            super.updateState();
        } finally {
            stateHolder.returnState(state);
        }
    }

    class CountStreamPreState extends StreamPreState {

        protected volatile boolean successCondition = false;

        private volatile boolean startStateReset = false;

        @Override
        public boolean canDestroy() {
            return super.canDestroy();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> snapshot = super.snapshot();
            snapshot.put("SuccessCondition", successCondition);
            snapshot.put("StartStateReset", startStateReset);
            return snapshot;
        }

        @Override
        public void restore(Map<String, Object> state) {
            super.restore(state);
            successCondition = (Boolean) state.get("SuccessCondition");
            startStateReset = (Boolean) state.get("StartStateReset");
        }
    }
}

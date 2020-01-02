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
import io.siddhi.core.event.state.StateEventCloner;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The processor the gets executes before checking state conditions.
 */
public class StreamPreStateProcessor implements PreStateProcessor {

    protected int stateId;
    protected boolean isStartState;
    protected StateInputStream.Type stateType;
    protected long withinTime = SiddhiConstants.UNKNOWN_STATE;
    protected int[] startStateIds;
    protected PreStateProcessor withinEveryPreStateProcessor;
    protected StreamPostStateProcessor thisStatePostProcessor;
    protected StreamPostStateProcessor thisLastProcessor;
    protected Processor nextProcessor;

    protected ReentrantLock lock = new ReentrantLock();

    protected StateEventFactory stateEventFactory;
    protected StreamEventCloner streamEventCloner;
    protected StateEventCloner stateEventCloner;
    protected StreamEventFactory streamEventFactory;
    protected SiddhiQueryContext siddhiQueryContext;
    protected StateHolder<StreamPreState> stateHolder;
    protected Comparator eventTimeComparator = new Comparator<StateEvent>() {
        @Override
        public int compare(StateEvent o1, StateEvent o2) {
            if (o1.getTimestamp() == -1) {
                if (o2.getTimestamp() == -1) {
                    return 0;
                } else {
                    return 1;
                }
            } else if (o2.getTimestamp() == -1) {
                return -1;
            }
            return Long.compare(o1.getTimestamp(), o2.getTimestamp());
        }
    };

    public StreamPreStateProcessor(StateInputStream.Type stateType) {
        this.stateType = stateType;
    }

    public void init(SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        this.stateHolder = siddhiQueryContext.generateStateHolder(
                this.getClass().getName(),
                false, () -> new StreamPreState());
    }

    public StreamPostStateProcessor getThisStatePostProcessor() {
        return thisStatePostProcessor;
    }

    public void setThisStatePostProcessor(StreamPostStateProcessor thisStatePostProcessor) {
        this.thisStatePostProcessor = thisStatePostProcessor;
    }

    /**
     * Process the handed StreamEvent
     *
     * @param complexEventChunk event chunk to be processed
     */
    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        throw new IllegalStateException("process method of StreamPreStateProcessor should not be called. " +
                "processAndReturn method is used for handling event chunks.");
    }

    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {
        throw new IllegalStateException("process method of StreamPreStateProcessor should not be called. " +
                "processAndReturn method is used for handling event chunks.");
    }

    protected boolean isExpired(StateEvent pendingStateEvent, long currentTimestamp) {
        if (withinTime != SiddhiConstants.UNKNOWN_STATE) {
            for (int startStateId : startStateIds) {
                StreamEvent streamEvent = pendingStateEvent.getStreamEvent(startStateId);
                if (streamEvent != null && Math.abs(pendingStateEvent.getStreamEvent(startStateId).getTimestamp()
                        - currentTimestamp) > withinTime) {
                    return true;
                }
            }
        }
        return false;
    }

    protected void process(StateEvent stateEvent) {
        StreamPreState state = stateHolder.getState();
        try {
            state.currentStateEventChunk.add(stateEvent);
            state.currentStateEventChunk.reset();
            state.stateChanged = false;
            nextProcessor.process(state.currentStateEventChunk);
            state.currentStateEventChunk.reset();
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * Get next processor element in the processor chain. Processed event should be sent to next processor
     *
     * @return Processor next processor
     */
    @Override
    public Processor getNextProcessor() {
        return nextProcessor;
    }

    /**
     * Set next processor element in processor chain
     *
     * @param processor Processor to be set as next element of processor chain
     */
    @Override
    public void setNextProcessor(Processor processor) {
        this.nextProcessor = processor;
    }

    /**
     * Set as the last element of the processor chain
     *
     * @param processor Last processor in the chain
     */
    @Override
    public void setToLast(Processor processor) {
        if (nextProcessor == null) {
            this.nextProcessor = processor;
        } else {
            this.nextProcessor.setToLast(processor);
        }
    }

    public void init() {
        StreamPreState state = stateHolder.getState();
        try {
            if (isStartState && (!state.initialized ||
                    this.thisStatePostProcessor.nextEveryStatePreProcessor != null ||
                    (stateType == StateInputStream.Type.SEQUENCE && this.thisStatePostProcessor.nextStatePreProcessor
                            instanceof AbsentPreStateProcessor))) {
                // For 'every' sequence, the 'thisStatePostProcessor.nextEveryStatePreProcessor != null'
                // check is not enough
                StateEvent stateEvent = stateEventFactory.newInstance();
                addState(stateEvent);
                state.initialized = true;
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    public StreamPostStateProcessor getThisLastProcessor() {
        return thisLastProcessor;
    }

    public void setThisLastProcessor(StreamPostStateProcessor thisLastProcessor) {
        this.thisLastProcessor = thisLastProcessor;
    }

    @Override
    public void addState(StateEvent stateEvent) {
        StreamPreState state = stateHolder.getState();
        try {
            addState(stateEvent, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    protected void addState(StateEvent stateEvent, StreamPreState state) {
        lock.lock();
        try {
            if (stateType == StateInputStream.Type.SEQUENCE) {
                if (state.newAndEveryStateEventList.isEmpty()) {
                    state.newAndEveryStateEventList.add(stateEvent);
                }
            } else {
                state.newAndEveryStateEventList.add(stateEvent);
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
            clonedEvent.setType(ComplexEvent.Type.CURRENT);
            for (int i = stateId; i < clonedEvent.getStreamEvents().length; i++) {
                clonedEvent.setEvent(i, null);
            }
            StreamPreState state = stateHolder.getState();
            try {
                state.newAndEveryStateEventList.add(clonedEvent);
            } finally {
                stateHolder.returnState(state);
            }
        } finally {
            lock.unlock();
        }
    }

    public void setWithinEveryPreStateProcessor(PreStateProcessor withinEveryPreStateProcessor) {
        this.withinEveryPreStateProcessor = withinEveryPreStateProcessor;
    }

    public void stateChanged() {
        StreamPreState state = stateHolder.getState();
        try {
            state.stateChanged = true;
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public boolean isStartState() {
        return isStartState;
    }

    public void setStartState(boolean isStartState) {
        this.isStartState = isStartState;
    }

    public void setStateEventFactory(StateEventFactory stateEventFactory) {
        this.stateEventFactory = stateEventFactory;
    }

    public void setStreamEventFactory(StreamEventFactory streamEventFactory) {
        this.streamEventFactory = streamEventFactory;
    }

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventCloner = streamEventCloner;
    }

    public void setStateEventCloner(StateEventCloner stateEventCloner) {
        this.stateEventCloner = stateEventCloner;
    }

    @Override
    public void resetState() {
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            state.pendingStateEventList.clear();
            if (isStartState && state.newAndEveryStateEventList.isEmpty()) {
                if (stateType == StateInputStream.Type.SEQUENCE && thisStatePostProcessor.nextEveryStatePreProcessor ==
                        null && !((StreamPreStateProcessor) thisStatePostProcessor.nextStatePreProcessor).
                        getPendingStateEventList().isEmpty()) {
                    return;
                }
                init();
            }
        } finally {
            lock.unlock();
            stateHolder.returnState(state);
        }
    }

    @Override
    public void updateState() {
        lock.lock();
        try {
            StreamPreState state = stateHolder.getState();
            try {
                state.newAndEveryStateEventList.sort(eventTimeComparator);
                state.pendingStateEventList.addAll(state.newAndEveryStateEventList);
                state.newAndEveryStateEventList.clear();
            } finally {
                stateHolder.returnState(state);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void expireEvents(long timestamp) {
        StreamPreState state = stateHolder.getState();
        lock.lock();
        try {
            StateEvent expiredStateEvent = null;
            for (Iterator<StateEvent> iterator = state.pendingStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (isExpired(stateEvent, timestamp)) {
                    iterator.remove();
                    if (stateEvent.getType() != ComplexEvent.Type.EXPIRED) {
                        stateEvent.setType(ComplexEvent.Type.EXPIRED);
                        expiredStateEvent = stateEvent;
                    }
                } else {
                    break;
                }
            }
            for (Iterator<StateEvent> iterator = state.newAndEveryStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (isExpired(stateEvent, timestamp)) {
                    iterator.remove();
                    if (stateEvent.getType() != ComplexEvent.Type.EXPIRED) {
                        stateEvent.setType(ComplexEvent.Type.EXPIRED);
                        expiredStateEvent = stateEvent;
                    }
                }
            }
            if (expiredStateEvent != null && withinEveryPreStateProcessor != null) {
                withinEveryPreStateProcessor.addEveryState(expiredStateEvent);
                withinEveryPreStateProcessor.updateState();
            }
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
            for (Iterator<StateEvent> iterator = state.pendingStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                stateEvent.setEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                process(stateEvent);
                if (this.thisLastProcessor.isEventReturned()) {
                    this.thisLastProcessor.clearProcessedEvent();
                    returnEventChunk.add(stateEvent);
                }
                if (state.stateChanged) {
                    iterator.remove();
                } else {
                    switch (stateType) {
                        case PATTERN:
                            stateEvent.setEvent(stateId, null);
                            break;
                        case SEQUENCE:
                            stateEvent.setEvent(stateId, null);
                            if (removeOnNoStateChange(stateType)) {
                                iterator.remove();
                            }
                            if (thisStatePostProcessor.callbackPreStateProcessor != null) {
                                thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
                            }
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

    protected boolean removeOnNoStateChange(StateInputStream.Type stateType) {
        return stateType == StateInputStream.Type.SEQUENCE;
    }

    @Override
    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    public void setWithinTime(long withinTime) {
        this.withinTime = withinTime;
    }

    public void setStartStateIds(int[] stateIds) {
        this.startStateIds = stateIds;
    }

    public List<StateEvent> getPendingStateEventList() {
        StreamPreState state = stateHolder.getState();
        try {
            return state.pendingStateEventList;
        } finally {
            stateHolder.returnState(state);
        }
    }

    class StreamPreState extends State {
        private ComplexEventChunk<StateEvent> currentStateEventChunk = new ComplexEventChunk<StateEvent>();
        private LinkedList<StateEvent> pendingStateEventList = new LinkedList<StateEvent>();
        private LinkedList<StateEvent> newAndEveryStateEventList = new LinkedList<StateEvent>();
        private volatile boolean stateChanged = false;
        private boolean initialized;
        private boolean started;

        @Override
        public boolean canDestroy() {
            return currentStateEventChunk.getFirst() == null &&
                    pendingStateEventList.isEmpty() &&
                    newAndEveryStateEventList.isEmpty() && !initialized;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("FirstEvent", currentStateEventChunk.getFirst());
            state.put("PendingStateEventList", pendingStateEventList);
            state.put("NewAndEveryStateEventList", newAndEveryStateEventList);
            state.put("Initialized", initialized);
            state.put("Started", started);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            currentStateEventChunk.clear();
            currentStateEventChunk.add((StateEvent) state.get("FirstEvent"));
            pendingStateEventList = (LinkedList<StateEvent>) state.get("PendingStateEventList");
            newAndEveryStateEventList = (LinkedList<StateEvent>) state.get("NewAndEveryStateEventList");
            initialized = (Boolean) state.get("Initialized");
            started = (Boolean) state.get("Started");
        }

        public ComplexEventChunk<StateEvent> getCurrentStateEventChunk() {
            return currentStateEventChunk;
        }

        public LinkedList<StateEvent> getPendingStateEventList() {
            return pendingStateEventList;
        }

        public LinkedList<StateEvent> getNewAndEveryStateEventList() {
            return newAndEveryStateEventList;
        }

        public boolean isStateChanged() {
            return stateChanged;
        }

        public void setStateChanged(boolean stateChanged) {
            this.stateChanged = stateChanged;
        }

        public void started() {
            started = true;
        }

        public boolean isStarted() {
            return started;
        }
    }

}

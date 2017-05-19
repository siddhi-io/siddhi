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

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.state.StateEventCloner;
import org.wso2.siddhi.core.event.state.StateEventPool;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 12/17/14.
 */
public class StreamPreStateProcessor implements PreStateProcessor, Snapshotable {

    protected int stateId;
    protected boolean isStartState;
    protected volatile boolean stateChanged = false;
    protected StateInputStream.Type stateType;
    protected List<Map.Entry<Long, Set<Integer>>> withinStates;
    protected ExecutionPlanContext executionPlanContext;
    protected String elementId;
    protected StreamPostStateProcessor thisStatePostProcessor;
    protected StreamPostStateProcessor thisLastProcessor;

    protected Processor nextProcessor;

    protected ComplexEventChunk<StateEvent> currentStateEventChunk = new ComplexEventChunk<StateEvent>(false);
    protected LinkedList<StateEvent> pendingStateEventList = new LinkedList<StateEvent>();
    protected LinkedList<StateEvent> pendingAbsentStateEventList = new LinkedList<StateEvent>();
    protected LinkedList<StateEvent> newAndEveryStateEventList = new LinkedList<StateEvent>();
    protected StateEventPool stateEventPool;
    //  private StreamEventPool streamEventPool;
    protected StreamEventCloner streamEventCloner;
    protected StateEventCloner stateEventCloner;
    protected StreamEventPool streamEventPool;
    protected String queryName;
    private boolean absentPartner;
    private long absentPartnerTimeout;
    private AbsentStreamPreStateProcessor absentStreamPreStateProcessor;

    public StreamPreStateProcessor(StateInputStream.Type stateType, List<Map.Entry<Long, Set<Integer>>> withinStates) {
        this.stateType = stateType;
        this.withinStates = withinStates;
    }

    public void init(ExecutionPlanContext executionPlanContext, String queryName) {
        this.executionPlanContext = executionPlanContext;
        this.queryName = queryName;
        if (elementId == null) {
            this.elementId = "StreamPreStateProcessor-" + executionPlanContext.getElementIdGenerator().createNewId();
        }
        executionPlanContext.getSnapshotService().addSnapshotable(queryName, this);
    }

    public boolean isAbsentPartner() {
        return absentPartner;
    }

    public void setAbsentPartner(boolean absentPartner) {
        this.absentPartner = absentPartner;
    }

    public AbsentStreamPreStateProcessor getAbsentStreamPreStateProcessor() {
        return absentStreamPreStateProcessor;
    }

    public void setAbsentStreamPreStateProcessor(AbsentStreamPreStateProcessor absentStreamPreStateProcessor) {
        this.absentStreamPreStateProcessor = absentStreamPreStateProcessor;
    }

    public long getAbsentPartnerTimeout() {
        return absentPartnerTimeout;
    }

    public void setAbsentPartnerTimeout(long absentPartnerTimeout) {
        this.absentPartnerTimeout = absentPartnerTimeout;
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

    private boolean isExpired(StateEvent pendingStateEvent, StreamEvent incomingStreamEvent) {
        for (Map.Entry<Long, Set<Integer>> withinEntry : withinStates) {
            for (Integer withinStateId : withinEntry.getValue()) {
                if (withinStateId == SiddhiConstants.ANY) {
                    if (Math.abs(pendingStateEvent.getTimestamp() - incomingStreamEvent.getTimestamp()) > withinEntry
                            .getKey()) {
                        return true;
                    }
                } else {
                    if (Math.abs(pendingStateEvent.getStreamEvent(withinStateId).getTimestamp() - incomingStreamEvent
                            .getTimestamp()) > withinEntry.getKey()) {
                        return true;

                    }
                }
            }
        }
        return false;

    }

    protected void process(StateEvent stateEvent) {
        currentStateEventChunk.add(stateEvent);
        currentStateEventChunk.reset();
        stateChanged = false;
        nextProcessor.process(currentStateEventChunk);
        currentStateEventChunk.reset();
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
        if (isStartState) {
            StateEvent stateEvent = stateEventPool.borrowEvent();
            addState(stateEvent);
        }
    }

    public StreamPostStateProcessor getThisLastProcessor() {
        return thisLastProcessor;
    }

    public void setThisLastProcessor(StreamPostStateProcessor thisLastProcessor) {
        this.thisLastProcessor = thisLastProcessor;
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of StreamPreStateProcessor
     */
    @Override
    public PreStateProcessor cloneProcessor(String key) {
        StreamPreStateProcessor streamPreStateProcessor = new StreamPreStateProcessor(stateType, withinStates);
        cloneProperties(streamPreStateProcessor, key);
        streamPreStateProcessor.init(executionPlanContext, queryName);
        return streamPreStateProcessor;
    }

    protected void cloneProperties(StreamPreStateProcessor streamPreStateProcessor, String key) {
        streamPreStateProcessor.stateId = this.stateId;
        streamPreStateProcessor.elementId = this.elementId + "-" + key;
        streamPreStateProcessor.stateEventPool = this.stateEventPool;
        streamPreStateProcessor.streamEventCloner = this.streamEventCloner;
        streamPreStateProcessor.stateEventCloner = this.stateEventCloner;
        streamPreStateProcessor.streamEventPool = this.streamEventPool;
    }

    @Override
    public void addState(StateEvent stateEvent) {
        //        if (stateType == StateInputStream.Type.SEQUENCE) {
        //            newAndEveryStateEventList.clear();
        //            pendingStateEventList.clear();
        //        }
        if (stateType == StateInputStream.Type.SEQUENCE) {
            if (newAndEveryStateEventList.isEmpty()) {
                newAndEveryStateEventList.add(stateEvent);
            }
        } else {
            newAndEveryStateEventList.add(stateEvent);

        }
    }

    public void addAbsent(StateEvent stateEvent) {
        pendingAbsentStateEventList.add(stateEvent);
    }

    @Override
    public void addEveryState(StateEvent stateEvent) {
        newAndEveryStateEventList.add(stateEventCloner.copyStateEvent(stateEvent));
    }

    public void stateChanged() {
        stateChanged = true;
    }

    public void setStartState(boolean isStartState) {
        this.isStartState = isStartState;
    }

    public void setStateEventPool(StateEventPool stateEventPool) {
        this.stateEventPool = stateEventPool;
    }

    public void setStreamEventPool(StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
    }

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventCloner = streamEventCloner;
    }

    public void setStateEventCloner(StateEventCloner stateEventCloner) {
        this.stateEventCloner = stateEventCloner;
    }

    @Override
    public void resetState() {
        pendingStateEventList.clear();
        if (isStartState && newAndEveryStateEventList.isEmpty()) {
            //        if (isStartState && stateType == StateInputStream.Type.SEQUENCE && newAndEveryStateEventList
            // .isEmpty()) {
            init();
        }
    }

    @Override
    public void updateState() {
        pendingStateEventList.addAll(newAndEveryStateEventList);
        newAndEveryStateEventList.clear();
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<StateEvent>(false);
        complexEventChunk.reset();
        StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent

        if (absentPartner) {
            // Remove the expired events based on absent partner's timeout
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            for (Iterator<StateEvent> iterator = pendingStateEventList.iterator(); iterator.hasNext(); ) {
                StateEvent stateEvent = iterator.next();
                if (currentTime >= stateEvent.getTimestamp() + absentPartnerTimeout) {
                    iterator.remove();
                }
            }

            // If there were no events received by the absent partner
            if (!pendingAbsentStateEventList.isEmpty()) {
                processAndReturn(returnEventChunk, streamEvent, pendingAbsentStateEventList);
            } else if (pendingStateEventList.isEmpty() && !(this instanceof AbsentStreamPreStateProcessor) &&
                    !absentStreamPreStateProcessor.hasStateEvents()) {
                if (absentStreamPreStateProcessor.isNoPresentBeforeInPattern()) {
                    // Absent processor did not receive any events before the current event.
                    // Send it to the next pre processor if available.
                    StateEvent stateEvent = stateEventPool.borrowEvent();
                    stateEvent.setEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                    process(stateEvent);
                    if (this.thisLastProcessor.isEventReturned()) {
                        this.thisLastProcessor.clearProcessedEvent();
                        returnEventChunk.add(stateEvent);
                    }
                }
            }

            if (this instanceof AbsentStreamPreStateProcessor) {
                StateEvent stateEvent = stateEventPool.borrowEvent();
                stateEvent.setEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
                process(stateEvent);
                if (this.thisLastProcessor.isEventReturned()) {
                    this.thisLastProcessor.clearProcessedEvent();
                    returnEventChunk.add(stateEvent);
                }
            }
        } else {
            processAndReturn(returnEventChunk, streamEvent, pendingStateEventList);
        }
        return returnEventChunk;
    }

    private void processAndReturn(ComplexEventChunk<StateEvent> returnEventChunk, StreamEvent streamEvent,
                                  List<StateEvent> listToTraverse) {
        for (Iterator<StateEvent> iterator = listToTraverse.iterator(); iterator.hasNext(); ) {
            StateEvent stateEvent = iterator.next();
            // Get the timestamp before process because the timestamp of state event will be updated in post processor
            if (withinStates.size() > 0) {
                if (isExpired(stateEvent, streamEvent)) {
                    iterator.remove();
                    continue;
                }
            }

            // If this is is next to an absent processor, should not send the event that matched with an event
            // received by the absence processor within the given time.
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
                        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
                            thisStatePostProcessor.callbackPreStateProcessor.startStateReset();
                        }
                        break;
                }
            }
        }
    }

    @Override
    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("CurrentStateEventChunk", currentStateEventChunk.getFirst());
        state.put("PendingStateEventList", pendingStateEventList);
        state.put("NewAndEveryStateEventList", newAndEveryStateEventList);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        currentStateEventChunk.clear();
        currentStateEventChunk.add((StateEvent) state.get("FirstEvent"));
        pendingStateEventList = (LinkedList<StateEvent>) state.get("PendingStateEventList");
        newAndEveryStateEventList = (LinkedList<StateEvent>) state.get("NewAndEveryStateEventList");
    }

    @Override
    public String getElementId() {
        return elementId;
    }
}

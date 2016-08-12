/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.query.output.ratelimit.snapshot;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.state.StateEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.Schedulable;

public abstract class SnapshotOutputRateLimiter implements Schedulable {
    static final Logger log = Logger.getLogger(SnapshotOutputRateLimiter.class);
    private WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter;
    protected ExecutionPlanContext executionPlanContext;
    protected StreamEventCloner streamEventCloner;
    protected StateEventCloner stateEventCloner;
    private boolean receiveStreamEvent;
    protected LockWrapper lockWrapper;

    protected SnapshotOutputRateLimiter(WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter, ExecutionPlanContext executionPlanContext) {
        this.wrappedSnapshotOutputRateLimiter = wrappedSnapshotOutputRateLimiter;
        this.executionPlanContext = executionPlanContext;
    }

    public abstract void process(ComplexEventChunk complexEventChunk);

    public abstract SnapshotOutputRateLimiter clone(String key, WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter);

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventCloner = streamEventCloner;
        this.receiveStreamEvent = true;
    }

    public void setStateEventCloner(StateEventCloner stateEventCloner) {
        this.stateEventCloner = stateEventCloner;
        this.receiveStreamEvent = false;
    }

    protected void sendToCallBacks(ComplexEventChunk complexEventChunk) {
        wrappedSnapshotOutputRateLimiter.passToCallBacks(complexEventChunk);
    }

    /**
     * Clones a given complex event.
     * @param complexEvent Complex event to be cloned
     * @return Cloned complex event
     */
    protected ComplexEvent cloneComplexEvent(ComplexEvent complexEvent){
        if(receiveStreamEvent){
            return streamEventCloner.copyStreamEvent((StreamEvent) complexEvent);
        } else {
            return stateEventCloner.copyStateEvent((StateEvent) complexEvent);
        }
    }

    public abstract void start();

    public abstract void stop();

    public abstract Object[] currentState();

    public abstract void restoreState(Object[] state);

    public void setQueryLock(LockWrapper lockWrapper) {
        this.lockWrapper = lockWrapper;
    }
}

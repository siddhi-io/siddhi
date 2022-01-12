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

package io.siddhi.core.query.output.ratelimit.snapshot;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventCloner;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Parent implementation to run the {@link Scheduler} to handle periodic snapshot rate
 * limiting.
 *
 * @param <S> current state of the RateLimiter
 */
public abstract class SnapshotOutputRateLimiter<S extends State> implements Schedulable {
    private static final Logger log = LogManager.getLogger(SnapshotOutputRateLimiter.class);
    protected final StateHolder<S> stateHolder;
    protected StreamEventCloner streamEventCloner;
    protected StateEventCloner stateEventCloner;
    protected SiddhiQueryContext siddhiQueryContext;
    protected LockWrapper lockWrapper;
    private WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter;
    private boolean receiveStreamEvent;

    protected SnapshotOutputRateLimiter(WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter,
                                        SiddhiQueryContext siddhiQueryContext, boolean groupBy) {
        this.wrappedSnapshotOutputRateLimiter = wrappedSnapshotOutputRateLimiter;
        this.siddhiQueryContext = siddhiQueryContext;
        stateHolder = siddhiQueryContext.generateStateHolder(this.getClass().getName(), groupBy, init());
    }

    protected abstract StateFactory<S> init();

    public abstract void process(ComplexEventChunk complexEventChunk);

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventCloner = streamEventCloner;
        this.receiveStreamEvent = true;
    }

    public void setStateEventCloner(StateEventCloner stateEventCloner) {
        this.stateEventCloner = stateEventCloner;
        this.receiveStreamEvent = false;
    }

    protected void sendToCallBacks(List<ComplexEventChunk> outputEventChunks) {
        if (!outputEventChunks.isEmpty()) {
            ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
            for (ComplexEventChunk eventChunk : outputEventChunks) {
                outputEventChunk.addAll(eventChunk);
            }
            wrappedSnapshotOutputRateLimiter.passToCallBacks(outputEventChunk);
        }
    }

    /**
     * Clones a given complex event.
     *
     * @param complexEvent Complex event to be cloned
     * @return Cloned complex event
     */
    protected ComplexEvent cloneComplexEvent(ComplexEvent complexEvent) {
        if (receiveStreamEvent) {
            return streamEventCloner.copyStreamEvent((StreamEvent) complexEvent);
        } else {
            return stateEventCloner.copyStateEvent((StateEvent) complexEvent);
        }
    }

    public abstract void partitionCreated();

    public void setQueryLock(LockWrapper lockWrapper) {
        this.lockWrapper = lockWrapper;
    }
}

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
package io.siddhi.core.query.output.ratelimit;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.partition.PartitionCreationListener;
import io.siddhi.core.query.input.MultiProcessStreamReceiver;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.metrics.Level;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract parent implementation of Output Rate Limiting. Output Rate Limiting is used to throttle the output of
 * Siddhi queries based on various criteria.
 *
 * @param <S> current state of the RateLimiter
 */
public abstract class OutputRateLimiter<S extends State> implements PartitionCreationListener {

    protected List<QueryCallback> queryCallbacks = new ArrayList<QueryCallback>();
    protected OutputCallback outputCallback = null;
    protected LatencyTracker latencyTracker;
    protected SiddhiQueryContext siddhiQueryContext;
    protected LockWrapper lockWrapper;
    protected StateHolder<S> stateHolder;
    private boolean hasCallBack = false;

    public void init(LockWrapper lockWrapper, boolean groupBy, SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        if (outputCallback != null) {
            this.lockWrapper = lockWrapper;
        }
        latencyTracker = siddhiQueryContext.getLatencyTracker();
        stateHolder = siddhiQueryContext.generateStateHolder(this.getClass().getName(), groupBy, init());
    }

    protected abstract StateFactory<S> init();

    public void sendToCallBacks(ComplexEventChunk complexEventChunk) {
        MultiProcessStreamReceiver.ReturnEventHolder returnEventHolder =
                MultiProcessStreamReceiver.getMultiProcessReturn().get();
        if (Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0 &&
                latencyTracker != null) {
            latencyTracker.markOut();
        }
        if (returnEventHolder != null) {
            returnEventHolder.setReturnEvents(complexEventChunk);
            return;
        } else if (lockWrapper != null) {
            lockWrapper.unlock();
        }
        if (Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0 &&
                latencyTracker != null) {
            latencyTracker.markOut();
        }
        if (lockWrapper != null) {
            lockWrapper.unlock();
        }
        if (!queryCallbacks.isEmpty()) {
            for (QueryCallback callback : queryCallbacks) {
                callback.receiveStreamEvent(complexEventChunk);
            }
        }
        if (outputCallback != null && complexEventChunk.getFirst() != null) {
            complexEventChunk.reset();
            int noOfEvents = 0;
            while (complexEventChunk.hasNext()) {
                ComplexEvent complexEvent = complexEventChunk.next();
                if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                    complexEvent.setType(ComplexEvent.Type.CURRENT);
                    noOfEvents++;
                } else if (complexEvent.getType() == ComplexEvent.Type.RESET) {
                    complexEventChunk.remove();
                } else {
                    noOfEvents++;
                }
            }
            if (complexEventChunk.getFirst() != null) {
                outputCallback.send(complexEventChunk, noOfEvents);
            }
        }

    }

    public void addQueryCallback(QueryCallback callback) {
        queryCallbacks.add(callback);
        hasCallBack = true;
    }

    public void removeQueryCallback(QueryCallback callback) {
        List<QueryCallback> newQueryCallbacks = new ArrayList<>(queryCallbacks);
        newQueryCallbacks.remove(callback);
        queryCallbacks = newQueryCallbacks;
        if (queryCallbacks.size() == 0) {
            hasCallBack = false;
        }
    }

    public abstract void process(ComplexEventChunk complexEventChunk);

    public void process(List<ComplexEventChunk> complexEventChunks) {
        ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<>();
        for (ComplexEventChunk aComplexEventChunk : complexEventChunks) {
            complexEventChunk.addAll(aComplexEventChunk);
        }
        process(complexEventChunk);
    }

    public OutputCallback getOutputCallback() {
        return outputCallback;
    }

    public void setOutputCallback(OutputCallback outputCallback) {
        this.outputCallback = outputCallback;
        if (outputCallback != null) {
            hasCallBack = true;
        }
    }

    public boolean hasCallBack() {
        return hasCallBack;
    }

}


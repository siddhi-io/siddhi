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
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEventCloner;
import io.siddhi.core.event.state.StateEventPool;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventPool;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.selector.attribute.processor.AttributeProcessor;
import io.siddhi.core.query.selector.attribute.processor.executor.AbstractAggregationAttributeExecutor;
import io.siddhi.core.util.lock.LockWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Implementation of {@link OutputRateLimiter} to be used by Snapshot Output Rate Limiting implementations.
 */
public class WrappedSnapshotOutputRateLimiter extends OutputRateLimiter {
    private final Long value;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean groupBy;
    private final boolean windowed;
    private SiddhiQueryContext siddhiQueryContext;
    private SnapshotOutputRateLimiter outputRateLimiter;
    private String id;
    private List<Integer> aggregateAttributePositionList = new ArrayList<Integer>();

    public WrappedSnapshotOutputRateLimiter(String id, Long value, ScheduledExecutorService scheduledExecutorService,
                                            boolean isGroupBy, boolean isWindowed,
                                            SiddhiQueryContext siddhiQueryContext) {
        this.id = id;
        this.value = value;
        this.scheduledExecutorService = scheduledExecutorService;
        this.groupBy = isGroupBy;
        this.windowed = isWindowed;
        this.siddhiQueryContext = siddhiQueryContext;
    }

    @Override
    public OutputRateLimiter clone(String key) {
        WrappedSnapshotOutputRateLimiter instance = new WrappedSnapshotOutputRateLimiter(id + key, value,
                scheduledExecutorService, groupBy, windowed, siddhiQueryContext);
        instance.outputRateLimiter = this.outputRateLimiter.clone(key, instance);
        return instance;
    }

    public void init(SiddhiQueryContext siddhiQueryContext, LockWrapper lockWrapper) {
        super.init(lockWrapper, siddhiQueryContext);
        outputRateLimiter.setQueryLock(lockWrapper);
    }

    public void init(int outPutAttributeSize, List<AttributeProcessor> attributeProcessorList, MetaComplexEvent
            metaComplexEvent) {
        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
            if (attributeProcessor.getExpressionExecutor() instanceof AbstractAggregationAttributeExecutor) {
                aggregateAttributePositionList.add(attributeProcessor.getOutputPosition());
            }
        }

        if (windowed) {
            if (groupBy) {
                if (outPutAttributeSize == aggregateAttributePositionList.size()) {   //All Aggregation
                    outputRateLimiter = new AllAggregationGroupByWindowedPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, this,
                            siddhiQueryContext);
                } else if (aggregateAttributePositionList.size() > 0) {   //Some Aggregation
                    outputRateLimiter = new AggregationGroupByWindowedPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, aggregateAttributePositionList,
                            this, siddhiQueryContext);
                } else { // No aggregation
                    //GroupBy is same as Non GroupBy
                    outputRateLimiter = new WindowedPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, this,
                            siddhiQueryContext);
                }
            } else {
                if (outPutAttributeSize == aggregateAttributePositionList.size()) {   //All Aggregation
                    outputRateLimiter = new AllAggregationPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, this,
                            siddhiQueryContext);
                } else if (aggregateAttributePositionList.size() > 0) {   //Some Aggregation
                    outputRateLimiter = new AggregationWindowedPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, aggregateAttributePositionList,
                            this, siddhiQueryContext);
                } else { // No aggregation
                    outputRateLimiter = new WindowedPerSnapshotOutputRateLimiter(
                            id, value, scheduledExecutorService, this,
                            siddhiQueryContext);
                }
            }

        } else {
            if (groupBy) {
                outputRateLimiter = new GroupByPerSnapshotOutputRateLimiter(id, value, scheduledExecutorService,
                        this, siddhiQueryContext);
            } else {
                outputRateLimiter = new PerSnapshotOutputRateLimiter(id, value, scheduledExecutorService,
                        this, siddhiQueryContext);
            }
        }


        if (metaComplexEvent instanceof MetaStateEvent) {
            StateEventPool stateEventPool = new StateEventPool((MetaStateEvent) metaComplexEvent, 5);
            outputRateLimiter.setStateEventCloner(new StateEventCloner((MetaStateEvent) metaComplexEvent,
                    stateEventPool));
        } else {
            StreamEventPool streamEventPool = new StreamEventPool((MetaStreamEvent) metaComplexEvent, 5);
            outputRateLimiter.setStreamEventCloner(new StreamEventCloner((MetaStreamEvent) metaComplexEvent,
                    streamEventPool));
        }

    }


    @Override
    public void start() {
        outputRateLimiter.start();
    }

    @Override
    public void stop() {
        outputRateLimiter.stop();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        outputRateLimiter.process(complexEventChunk);
    }

    public void passToCallBacks(ComplexEventChunk complexEventChunk) {
        sendToCallBacks(complexEventChunk);
    }

    @Override
    public Map<String, Object> currentState() {
        return outputRateLimiter.currentState();
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        outputRateLimiter.restoreState(state);
    }
}

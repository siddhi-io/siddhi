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
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;
import io.siddhi.core.query.selector.attribute.processor.AttributeProcessor;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link OutputRateLimiter} to be used by Snapshot Output Rate Limiting implementations.
 *
 * @param <S> current state of the RateLimiter
 */
public class WrappedSnapshotOutputRateLimiter<S extends State> extends OutputRateLimiter<S> {
    private final Long value;
    private final boolean groupBy;
    private final boolean windowed;
    private SiddhiQueryContext siddhiQueryContext;
    private SnapshotOutputRateLimiter outputRateLimiter;
    private List<Integer> aggregateAttributePositionList = new ArrayList<Integer>();

    public WrappedSnapshotOutputRateLimiter(Long value,
                                            boolean groupBy, boolean isWindowed,
                                            SiddhiQueryContext siddhiQueryContext) {
        this.value = value;
        this.groupBy = groupBy;
        this.windowed = isWindowed;
        this.siddhiQueryContext = siddhiQueryContext;
    }

    public void init(SiddhiQueryContext siddhiQueryContext, LockWrapper lockWrapper) {
        super.init(lockWrapper, groupBy, siddhiQueryContext);
        outputRateLimiter.setQueryLock(lockWrapper);
    }

    public void init(int outPutAttributeSize, List<AttributeProcessor> attributeProcessorList, MetaComplexEvent
            metaComplexEvent) {
        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
            if (attributeProcessor.getExpressionExecutor() instanceof AttributeAggregatorExecutor<?>) {
                aggregateAttributePositionList.add(attributeProcessor.getOutputPosition());
            }
        }

        if (windowed) {
            if (groupBy) {
                if (outPutAttributeSize == aggregateAttributePositionList.size()) {   //All Aggregation
                    outputRateLimiter = new AllAggregationGroupByWindowedPerSnapshotOutputRateLimiter(
                            value, this, groupBy,
                            siddhiQueryContext);
                } else if (aggregateAttributePositionList.size() > 0) {   //Some Aggregation
                    outputRateLimiter = new AggregationGroupByWindowedPerSnapshotOutputRateLimiter(
                            value, aggregateAttributePositionList,
                            this, groupBy, siddhiQueryContext);
                } else { // No aggregation
                    //GroupBy is same as Non GroupBy
                    outputRateLimiter = new WindowedPerSnapshotOutputRateLimiter(
                            value, this, groupBy,
                            siddhiQueryContext);
                }
            } else {
                if (outPutAttributeSize == aggregateAttributePositionList.size()) {   //All Aggregation
                    outputRateLimiter = new AllAggregationPerSnapshotOutputRateLimiter(
                            value, this, groupBy,
                            siddhiQueryContext);
                } else if (aggregateAttributePositionList.size() > 0) {   //Some Aggregation
                    outputRateLimiter = new AggregationWindowedPerSnapshotOutputRateLimiter(
                            value, aggregateAttributePositionList,
                            this, groupBy, siddhiQueryContext);
                } else { // No aggregation
                    outputRateLimiter = new WindowedPerSnapshotOutputRateLimiter(
                            value, this,
                            groupBy, siddhiQueryContext);
                }
            }

        } else {
            if (groupBy) {
                outputRateLimiter = new GroupByPerSnapshotOutputRateLimiter(value,
                        this, groupBy, siddhiQueryContext);
            } else {
                outputRateLimiter = new PerSnapshotOutputRateLimiter(value,
                        this, groupBy, siddhiQueryContext);
            }
        }


        if (metaComplexEvent instanceof MetaStateEvent) {
            StateEventFactory stateEventFactory = new StateEventFactory((MetaStateEvent) metaComplexEvent);
            outputRateLimiter.setStateEventCloner(new StateEventCloner((MetaStateEvent) metaComplexEvent,
                    stateEventFactory));
        } else {
            StreamEventFactory streamEventFactory = new StreamEventFactory((MetaStreamEvent) metaComplexEvent);
            outputRateLimiter.setStreamEventCloner(new StreamEventCloner((MetaStreamEvent) metaComplexEvent,
                    streamEventFactory));
        }

    }

    @Override
    public void partitionCreated() {
        outputRateLimiter.partitionCreated();
    }

    @Override
    protected StateFactory<S> init() {
        return null;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        outputRateLimiter.process(complexEventChunk);
    }

    public void passToCallBacks(ComplexEventChunk complexEventChunk) {
        sendToCallBacks(complexEventChunk);
    }

}

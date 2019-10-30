/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.aggregation;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.incremental.IncrementalUnixTimeFunctionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;

import java.util.List;

/**
 * Incremental Aggregation Processor to consume events to Incremental Aggregators.
 */
public class IncrementalAggregationProcessor implements Processor {
    private final List<ExpressionExecutor> incomingExpressionExecutors;
    private final StreamEventFactory streamEventFactory;
    private final LatencyTracker latencyTrackerInsert;
    private final ThroughputTracker throughputTrackerInsert;
    private SiddhiAppContext siddhiAppContext;
    private AggregationRuntime aggregationRuntime;

    private boolean isFirstEventArrived;

    public IncrementalAggregationProcessor(AggregationRuntime aggregationRuntime,
                                           List<ExpressionExecutor> incomingExpressionExecutors,
                                           MetaStreamEvent processedMetaStreamEvent,
                                           LatencyTracker latencyTrackerInsert,
                                           ThroughputTracker throughputTrackerInsert,
                                           SiddhiAppContext siddhiAppContext) {
        this.isFirstEventArrived = false;
        this.aggregationRuntime = aggregationRuntime;
        this.incomingExpressionExecutors = incomingExpressionExecutors;
        this.streamEventFactory = new StreamEventFactory(processedMetaStreamEvent);
        this.latencyTrackerInsert = latencyTrackerInsert;
        this.throughputTrackerInsert = throughputTrackerInsert;
        this.siddhiAppContext = siddhiAppContext;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<StreamEvent> streamEventChunk =
                new ComplexEventChunk<>();
        try {
            int noOfEvents = 0;
            if (latencyTrackerInsert != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerInsert.markIn();
            }
            while (complexEventChunk.hasNext()) {
                ComplexEvent complexEvent = complexEventChunk.next();
                if (!isFirstEventArrived) {
                    aggregationRuntime.initialiseExecutors(true);
                    isFirstEventArrived = true;
                }
                StreamEvent newEvent = streamEventFactory.newInstance();
                for (int i = 0; i < incomingExpressionExecutors.size(); i++) {
                    ExpressionExecutor expressionExecutor = incomingExpressionExecutors.get(i);
                    Object outputData = expressionExecutor.execute(complexEvent);
                    if (expressionExecutor instanceof IncrementalUnixTimeFunctionExecutor && outputData == null) {
                        throw new SiddhiAppRuntimeException("Cannot retrieve the timestamp of event");
                    }
                    newEvent.setOutputData(outputData, i);
                }
                streamEventChunk.add(newEvent);
                noOfEvents++;
            }
            aggregationRuntime.processEvents(streamEventChunk);
            if (throughputTrackerInsert != null &&
                    Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                throughputTrackerInsert.eventsIn(noOfEvents);
            }
        } finally {
            if (latencyTrackerInsert != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerInsert.markOut();
            }
        }

    }

    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        for (ComplexEventChunk streamEventChunk : complexEventChunks) {
            complexEventChunk.addAll(streamEventChunk);
        }
        process(complexEventChunk);
    }

    @Override
    public Processor getNextProcessor() {
        return null;
    }

    @Override
    public void setNextProcessor(Processor processor) {
        throw new SiddhiAppCreationException("IncrementalAggregationProcessor does not support any next processor");
    }

    @Override
    public void setToLast(Processor processor) {
        throw new SiddhiAppCreationException("IncrementalAggregationProcessor does not support any " +
                "next/last processor");
    }
}

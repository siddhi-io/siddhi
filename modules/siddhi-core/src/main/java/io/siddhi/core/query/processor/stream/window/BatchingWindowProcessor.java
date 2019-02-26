/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.query.processor.stream.window;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Performs event processing in a tumbling manner
 */
public abstract class BatchingWindowProcessor extends WindowProcessor {

    @Override
    protected List<Attribute> init(MetaStreamEvent metaStreamEvent,
                                   AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader,
                                   boolean outputExpectsExpiredEvents, SiddhiQueryContext siddhiQueryContext) {
        init(attributeExpressionExecutors, configReader, outputExpectsExpiredEvents, siddhiQueryContext);
        return new ArrayList<Attribute>(0);
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 the config reader of window
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param siddhiQueryContext           the context of the siddhi query
     */
    protected abstract void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                 boolean outputExpectsExpiredEvents, SiddhiQueryContext siddhiQueryContext);

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        streamEventChunk.reset();
        process(streamEventChunk, nextProcessor, streamEventCloner);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    protected abstract void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                    StreamEventCloner streamEventCloner);

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}

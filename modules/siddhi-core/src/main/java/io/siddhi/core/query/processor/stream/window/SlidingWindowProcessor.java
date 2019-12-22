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
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;

/**
 * Performs event processing in a sliding manner
 *
 * @param <S> current state of the processor
 */
public abstract class SlidingWindowProcessor<S extends State> extends WindowProcessor<S> {

    @Override
    protected StateFactory<S> init(MetaStreamEvent metaStreamEvent,
                                   AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, StreamEventClonerHolder streamEventClonerHolder,
                                   boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                   SiddhiQueryContext siddhiQueryContext) {
        return init(attributeExpressionExecutors, configReader, siddhiQueryContext);
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 the config reader of window
     * @param siddhiQueryContext           the context of the siddhi query
     */
    protected abstract StateFactory<S> init(ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader,
                                            SiddhiQueryContext siddhiQueryContext);

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     S state) {
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            streamEvent.setBeforeWindowData(null);
        }
        streamEventChunk.reset();
        process(streamEventChunk, nextProcessor, streamEventCloner, state);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     * @param state             current query state
     */
    protected abstract void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                    StreamEventCloner streamEventCloner, S state);

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.SLIDE;
    }
}

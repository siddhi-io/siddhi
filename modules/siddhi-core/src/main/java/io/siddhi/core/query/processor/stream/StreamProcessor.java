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
package io.siddhi.core.query.processor.stream;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.snapshot.state.State;

/**
 * For Siddhi extensions, extend this class to use the functionality of
 * AbstractStreamProcessor. This class processes only StreamEvents. Use
 * StreamFunctionProcessor to process StateEvents.
 *
 * @param <S> current state of the processor
 */
public abstract class StreamProcessor<S extends State> extends AbstractStreamProcessor<S> {

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     S state) {
        streamEventChunk.reset();
        process(streamEventChunk, nextProcessor, streamEventCloner, complexEventPopulater, state);
    }


    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     * @param state                 current processor state
     */
    protected abstract void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                    StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                    S state);

}

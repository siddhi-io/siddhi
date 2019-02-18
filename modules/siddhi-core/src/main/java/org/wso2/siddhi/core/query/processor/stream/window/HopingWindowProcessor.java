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

package org.wso2.siddhi.core.query.processor.stream.window;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.event.stream.populater.SelectiveComplexEventPopulater;
import org.wso2.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Performs event processing in a hopping manner
 */
public abstract class HopingWindowProcessor extends WindowProcessor {

    protected List<Attribute> internalAttributes;
    protected HopingTimestampPopulator hopingTimestampPopulator;

    @Override
    protected List<Attribute> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                                   boolean outputExpectsExpiredEvents) {
        init(attributeExpressionExecutors, configReader, outputExpectsExpiredEvents, siddhiAppContext);
        Attribute groupingKey = new Attribute("_hopingTimestamp", Attribute.Type.STRING);
        internalAttributes = new ArrayList<Attribute>(1);
        internalAttributes.add(groupingKey);
        metaStreamEvent.addData(groupingKey);
        return new ArrayList<>(0);
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 the config reader of window
     * @param outputExpectsExpiredEvents   is expired event out put or not
     * @param siddhiAppContext             the context of the siddhi app
     */
    protected abstract void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                 boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext);

    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner,
                                     ComplexEventPopulater complexEventPopulater) {
        processEventChunk(streamEventChunk, nextProcessor, streamEventCloner, hopingTimestampPopulator);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk         the event chunk that need to be processed
     * @param nextProcessor            the next processor to which the success events need to be passed
     * @param streamEventCloner        helps to clone the incoming event for local storage or modification
     * @param hopingTimestampPopulator helps to populate the events with the hoping timestamp
     */
    protected abstract void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk,
                                              Processor nextProcessor, StreamEventCloner streamEventCloner,
                                              HopingTimestampPopulator hopingTimestampPopulator);

    public void constructStreamEventPopulater(MetaStreamEvent metaStreamEvent, int streamEventChainIndex) {
        super.constructStreamEventPopulater(metaStreamEvent, streamEventChainIndex);
        if (this.hopingTimestampPopulator == null) {
            hopingTimestampPopulator = new HopingTimestampPopulator(StreamEventPopulaterFactory.constructEventPopulator(
                    metaStreamEvent, streamEventChainIndex, internalAttributes));
        }
    }

    /**
     * Populates hopping timestamp to the complex event
     */
    public class HopingTimestampPopulator {

        private SelectiveComplexEventPopulater selectiveComplexEventPopulater;

        public HopingTimestampPopulator(SelectiveComplexEventPopulater selectiveComplexEventPopulater) {
            this.selectiveComplexEventPopulater = selectiveComplexEventPopulater;
        }

        public void populateComplexEvent(ComplexEvent complexEvent, String key) {
            selectiveComplexEventPopulater.populateComplexEvent(complexEvent, new Object[]{key});
        }

    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.HOP;
    }
}

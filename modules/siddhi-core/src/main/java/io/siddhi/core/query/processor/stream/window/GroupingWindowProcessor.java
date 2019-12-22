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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.event.stream.populater.SelectiveComplexEventPopulater;
import io.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Performs event processing with key based event groups
 *
 * @param <S> current state of the processor
 */
public abstract class GroupingWindowProcessor<S extends State> extends WindowProcessor<S> {

    protected List<Attribute> internalAttributes;
    protected GroupingKeyPopulator groupingKeyPopulator;

    @Override
    protected StateFactory<S> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, StreamEventClonerHolder streamEventClonerHolder,
                                   boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                   SiddhiQueryContext siddhiQueryContext) {
        StateFactory<S> stateFactory = init(attributeExpressionExecutors, configReader, outputExpectsExpiredEvents,
                siddhiQueryContext);
        Attribute groupingKey = new Attribute("_groupingKey", Attribute.Type.STRING);
        internalAttributes = new ArrayList<Attribute>(1);
        internalAttributes.add(groupingKey);
        metaStreamEvent.addData(groupingKey);
        return stateFactory;
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 the config reader of window
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param siddhiQueryContext           the context of the siddhi query
     */
    protected abstract StateFactory<S> init(ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                            SiddhiQueryContext siddhiQueryContext);

    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner,
                                     ComplexEventPopulater complexEventPopulater, S state) {
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            streamEvent.setBeforeWindowData(null);
        }
        streamEventChunk.reset();
        processEventChunk(streamEventChunk, nextProcessor, streamEventCloner, groupingKeyPopulator, state);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk     the event chunk that need to be processed
     * @param nextProcessor        the next processor to which the success events need to be passed
     * @param streamEventCloner    helps to clone the incoming event for local storage or modification
     * @param groupingKeyPopulater helps to populate the events with the grouping key
     * @param state                current state of the processor
     */
    protected abstract void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk,
                                              Processor nextProcessor, StreamEventCloner streamEventCloner,
                                              GroupingKeyPopulator groupingKeyPopulater, S state);

    public void constructStreamEventPopulater(MetaStreamEvent metaStreamEvent, int streamEventChainIndex) {
        super.constructStreamEventPopulater(metaStreamEvent, streamEventChainIndex);
        if (this.groupingKeyPopulator == null) {
            groupingKeyPopulator = new GroupingKeyPopulator(StreamEventPopulaterFactory.constructEventPopulator(
                    metaStreamEvent, streamEventChainIndex, internalAttributes));
        }
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.GROUP;
    }

    /**
     * Populates grouping key to the complex event
     */
    public class GroupingKeyPopulator {

        private SelectiveComplexEventPopulater selectiveComplexEventPopulater;

        public GroupingKeyPopulator(SelectiveComplexEventPopulater selectiveComplexEventPopulater) {
            this.selectiveComplexEventPopulater = selectiveComplexEventPopulater;
        }

        public void populateComplexEvent(ComplexEvent complexEvent, String key) {
            selectiveComplexEventPopulater.populateComplexEvent(complexEvent, new Object[]{key});
        }

    }
}

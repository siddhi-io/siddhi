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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.holder.ExternalReferencedHolder;
import io.siddhi.core.util.extension.validator.InputParameterValidator;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Abstract implementation of {@link Processor} intended to be used by any Stream Processors.
 *
 * @param <S> current state of the processor
 */
public abstract class AbstractStreamProcessor<S extends State> implements Processor, ExternalReferencedHolder {

    private static final Logger log = LogManager.getLogger(AbstractStreamProcessor.class);

    protected Processor nextProcessor;
    protected MetaStreamEvent metaStreamEvent;
    protected SiddhiQueryContext siddhiQueryContext;
    protected StreamEventClonerHolder streamEventClonerHolder = new StreamEventClonerHolder();
    protected AbstractDefinition inputDefinition;
    protected ExpressionExecutor[] attributeExpressionExecutors;
    protected int attributeExpressionLength;
    protected ComplexEventPopulater complexEventPopulater;
    protected StateHolder<S> stateHolder;
    private List<Attribute> additionalAttributes;

    public void initProcessor(MetaStreamEvent metaStreamEvent,
                              ExpressionExecutor[] attributeExpressionExecutors,
                              ConfigReader configReader,
                              boolean outputExpectsExpiredEvents,
                              boolean findToBeExecuted,
                              boolean groupBy, SiddhiElement siddhiElement,
                              SiddhiQueryContext siddhiQueryContext) {
        this.metaStreamEvent = metaStreamEvent;
        this.siddhiQueryContext = siddhiQueryContext;
        try {
            this.inputDefinition = metaStreamEvent.getLastInputDefinition();
            this.attributeExpressionExecutors = attributeExpressionExecutors;
            this.attributeExpressionLength = attributeExpressionExecutors.length;
            InputParameterValidator.validateExpressionExecutors(this, attributeExpressionExecutors);
            StateFactory<S> stateFactory = init(metaStreamEvent, metaStreamEvent.getLastInputDefinition(),
                    attributeExpressionExecutors, configReader, streamEventClonerHolder, outputExpectsExpiredEvents,
                    findToBeExecuted, siddhiQueryContext);
            this.additionalAttributes = getReturnAttributes();
            this.stateHolder = siddhiQueryContext.generateStateHolder(this.getClass().getName(), groupBy, stateFactory);
            siddhiQueryContext.getSiddhiAppContext().addEternalReferencedHolder(this);
            if (additionalAttributes.size() > 0) {
                StreamDefinition outputDefinition = StreamDefinition.id(inputDefinition.getId());
                outputDefinition.setQueryContextStartIndex(siddhiElement.getQueryContextStartIndex());
                outputDefinition.setQueryContextEndIndex(siddhiElement.getQueryContextEndIndex());
                for (Attribute attribute : inputDefinition.getAttributeList()) {
                    outputDefinition.attribute(attribute.getName(), attribute.getType());
                }
                for (Attribute attribute : additionalAttributes) {
                    outputDefinition.attribute(attribute.getName(), attribute.getType());
                }

                metaStreamEvent.addInputDefinition(outputDefinition);
            }
        } catch (Throwable t) {
            throw new SiddhiAppCreationException(t.getMessage(), t,
                    siddhiElement.getQueryContextStartIndex(),
                    siddhiElement.getQueryContextEndIndex(), siddhiQueryContext.getSiddhiAppContext());
        }
    }

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param metaStreamEvent              the stream event meta
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 this hold the {@link AbstractStreamProcessor} extensions configuration
     * @param streamEventClonerHolder      stream event cloner holder
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param findToBeExecuted             find will be executed
     * @param siddhiQueryContext           current siddhi query context
     */
    protected abstract StateFactory<S> init(MetaStreamEvent metaStreamEvent,
                                            AbstractDefinition inputDefinition,
                                            ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader,
                                            StreamEventClonerHolder streamEventClonerHolder,
                                            boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                            SiddhiQueryContext siddhiQueryContext);

    public abstract List<Attribute> getReturnAttributes();

    public void process(ComplexEventChunk streamEventChunk) {
        streamEventChunk.reset();
        S state = stateHolder.getState();
        try {
            processEventChunk(streamEventChunk, nextProcessor, streamEventClonerHolder.getStreamEventCloner(),
                    complexEventPopulater, state);
        } finally {
            stateHolder.returnState(state);
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

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     * @param state                 current state of the processor
     */
    protected abstract void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk,
                                              Processor nextProcessor,
                                              StreamEventCloner streamEventCloner,
                                              ComplexEventPopulater complexEventPopulater,
                                              S state);

    public Processor getNextProcessor() {
        return nextProcessor;
    }

    public void setNextProcessor(Processor processor) {
        this.nextProcessor = processor;
    }

    public void constructStreamEventPopulater(MetaStreamEvent metaStreamEvent, int streamEventChainIndex) {
        if (this.complexEventPopulater == null) {
            this.complexEventPopulater = StreamEventPopulaterFactory.constructEventPopulator(metaStreamEvent,
                    streamEventChainIndex,
                    additionalAttributes);
        }
    }

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventClonerHolder.setStreamEventCloner(streamEventCloner);
    }

    public void setToLast(Processor processor) {
        if (nextProcessor == null) {
            this.nextProcessor = processor;
        } else {
            this.nextProcessor.setToLast(processor);
        }
    }

    /**
     * Defines the behaviour of the processing, will be called after the init
     *
     * @return ProcessingMode processing mode of the processor
     */
    public abstract ProcessingMode getProcessingMode();

    public boolean isStateful() {
        return siddhiQueryContext.isStateful();
    }
}

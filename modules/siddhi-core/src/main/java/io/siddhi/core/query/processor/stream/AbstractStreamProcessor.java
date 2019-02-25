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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.event.stream.populater.StreamEventPopulaterFactory;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.holder.EternalReferencedHolder;
import io.siddhi.core.util.snapshot.Snapshotable;
import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Abstract implementation of {@link Processor} intended to be used by any Stream Processors.
 */
public abstract class AbstractStreamProcessor implements Processor, EternalReferencedHolder, Snapshotable {

    private static final Logger log = Logger.getLogger(AbstractStreamProcessor.class);

    protected Processor nextProcessor;

    protected List<Attribute> additionalAttributes;
    protected MetaStreamEvent metaStreamEvent;
    protected StreamEventClonerHolder streamEventClonerHolder = new StreamEventClonerHolder();
    protected StreamEventCloner streamEventCloner;
    protected AbstractDefinition inputDefinition;
    protected ExpressionExecutor[] attributeExpressionExecutors;
    protected SiddhiAppContext siddhiAppContext;
    protected int attributeExpressionLength;
    protected ComplexEventPopulater complexEventPopulater;
    protected String elementId = null;
    protected String queryName;
    private ConfigReader configReader;
    private boolean outputExpectsExpiredEvents;

    public void initProcessor(MetaStreamEvent metaStreamEvent,
                              ExpressionExecutor[] attributeExpressionExecutors,
                              ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                              boolean outputExpectsExpiredEvents,
                              String queryName, SiddhiElement siddhiElement) {
        this.configReader = configReader;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.metaStreamEvent = metaStreamEvent;
        try {
            this.inputDefinition = metaStreamEvent.getLastInputDefinition();
            this.attributeExpressionExecutors = attributeExpressionExecutors;
            this.siddhiAppContext = siddhiAppContext;
            this.attributeExpressionLength = attributeExpressionExecutors.length;
            this.queryName = queryName;
            if (elementId == null) {
                elementId = "AbstractStreamProcessor-" + siddhiAppContext.getElementIdGenerator().createNewId();
            }
            siddhiAppContext.getSnapshotService().addSnapshotable(queryName, this);
            this.additionalAttributes = init(metaStreamEvent, metaStreamEvent.getLastInputDefinition(),
                    attributeExpressionExecutors, configReader, siddhiAppContext, outputExpectsExpiredEvents);

            siddhiAppContext.addEternalReferencedHolder(this);

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
                    siddhiElement.getQueryContextEndIndex(), siddhiAppContext);
        }
    }

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param metaStreamEvent              the stream event meta
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 this hold the {@link AbstractStreamProcessor} extensions configuration
     *                                     reader.
     * @param siddhiAppContext             the context of the siddhi app
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @return the additional output attributes introduced by the function
     */
    protected abstract List<Attribute> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                            ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                                            boolean outputExpectsExpiredEvents);

    public void process(ComplexEventChunk streamEventChunk) {
        streamEventChunk.reset();
        processEventChunk(streamEventChunk, nextProcessor, streamEventCloner, complexEventPopulater);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    protected abstract void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                              StreamEventCloner streamEventCloner,
                                              ComplexEventPopulater complexEventPopulater);

    public Processor getNextProcessor() {
        return nextProcessor;
    }

    public void setNextProcessor(Processor processor) {
        this.nextProcessor = processor;
    }

    @Override
    public Processor cloneProcessor(String key) {
        try {
            AbstractStreamProcessor abstractStreamProcessor = this.getClass().newInstance();
            abstractStreamProcessor.inputDefinition = inputDefinition;
            ExpressionExecutor[] innerExpressionExecutors = new ExpressionExecutor[attributeExpressionLength];
            ExpressionExecutor[] attributeExpressionExecutors1 = this.attributeExpressionExecutors;
            for (int i = 0; i < attributeExpressionLength; i++) {
                innerExpressionExecutors[i] = attributeExpressionExecutors1[i].cloneExecutor(key);
            }
            abstractStreamProcessor.attributeExpressionExecutors = innerExpressionExecutors;
            abstractStreamProcessor.attributeExpressionLength = attributeExpressionLength;
            abstractStreamProcessor.additionalAttributes = additionalAttributes;
            abstractStreamProcessor.additionalAttributes = additionalAttributes;
            abstractStreamProcessor.complexEventPopulater = complexEventPopulater;
            abstractStreamProcessor.siddhiAppContext = siddhiAppContext;
            abstractStreamProcessor.elementId = elementId + "-" + key;
            abstractStreamProcessor.configReader = configReader;
            abstractStreamProcessor.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
            abstractStreamProcessor.queryName = queryName;
            abstractStreamProcessor.siddhiAppContext.getSnapshotService().addSnapshotable(queryName,
                    abstractStreamProcessor);
            abstractStreamProcessor.siddhiAppContext.addEternalReferencedHolder(abstractStreamProcessor);

            abstractStreamProcessor.init(metaStreamEvent, inputDefinition, attributeExpressionExecutors, configReader,
                    siddhiAppContext, outputExpectsExpiredEvents);
            abstractStreamProcessor.start();
            return abstractStreamProcessor;

        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception in cloning " + this.getClass().getCanonicalName(), e);
        }
    }

    public void constructStreamEventPopulater(MetaStreamEvent metaStreamEvent, int streamEventChainIndex) {
        if (this.complexEventPopulater == null) {
            this.complexEventPopulater = StreamEventPopulaterFactory.constructEventPopulator(metaStreamEvent,
                    streamEventChainIndex,
                    additionalAttributes);
        }
    }

    public void setStreamEventCloner(StreamEventCloner streamEventCloner) {
        this.streamEventCloner = streamEventCloner;
        this.streamEventClonerHolder.setStreamEventCloner(streamEventCloner);
    }

    public void setToLast(Processor processor) {
        if (nextProcessor == null) {
            this.nextProcessor = processor;
        } else {
            this.nextProcessor.setToLast(processor);
        }
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    @Override
    public void clean() {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            expressionExecutor.clean();
        }
        siddhiAppContext.getSnapshotService().removeSnapshotable(queryName, this);
    }

    /**
     * Defines the behaviour of the processing, will be called after the init
     *
     * @return ProcessingMode processing mode of the processor
     */
    public abstract ProcessingMode getProcessingMode();

}

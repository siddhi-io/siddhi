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
package io.siddhi.core.query.processor.stream.function;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.AbstractStreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;

/**
 * Stream Processor to handle Stream Functions.
 *
 * @param <S> current state of the processor
 */
public abstract class StreamFunctionProcessor<S extends State> extends AbstractStreamProcessor<S> {

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     S state) {
        while (streamEventChunk.hasNext()) {
            ComplexEvent complexEvent = streamEventChunk.next();
            Object[] outputData;
            switch (attributeExpressionLength) {
                case 0:
                    outputData = process((Object) null);
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    break;
                case 1:
                    outputData = process(attributeExpressionExecutors[0].execute(complexEvent));
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    break;
                default:
                    Object[] inputData = new Object[attributeExpressionLength];
                    for (int i = 0; i < attributeExpressionLength; i++) {
                        inputData[i] = attributeExpressionExecutors[i].execute(complexEvent);
                    }
                    outputData = process(inputData);
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
            }
        }
        nextProcessor.process(streamEventChunk);

    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    /**
     * The process method of the StreamFunction, used when more then one function parameters are provided
     *
     * @param data the data values for the function parameters
     * @return the data for additional output attributes introduced by the function
     */
    protected abstract Object[] process(Object[] data);


    /**
     * The process method of the StreamFunction, used when zero or one function parameter is provided
     *
     * @param data null if the function parameter count is zero or runtime data value of the function parameter
     * @return the data for additional output attribute introduced by the function
     */
    protected abstract Object[] process(Object data);

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param metaStreamEvent              the stream event meta
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 this hold the {@link StreamFunctionProcessor} extensions configuration
     * @param streamEventClonerHolder      stream event cloner Holder
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param findToBeExecuted             find operation will be performed
     * @param siddhiQueryContext           siddhi query context
     */
    protected StateFactory<S> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   StreamEventClonerHolder streamEventClonerHolder,
                                   boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                   SiddhiQueryContext siddhiQueryContext) {
        return init(inputDefinition, attributeExpressionExecutors, configReader, outputExpectsExpiredEvents,
                siddhiQueryContext);
    }

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param configReader                 this hold the {@link StreamFunctionProcessor} extensions configuration
     *                                     reader.
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param siddhiQueryContext           the context of the siddhi query
     * @return the additional output attributes introduced by the function
     */
    protected abstract StateFactory<S> init(AbstractDefinition inputDefinition,
                                            ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                            SiddhiQueryContext siddhiQueryContext);

}

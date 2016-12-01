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

package org.wso2.carbon.ml.siddhi.extension.streamingml;

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.regression.StreamingRegression;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class StreamingRegressionExtension extends StreamProcessor implements
        SchedulingProcessor {

    private int maxInstance;
    private int batchSize;
    private int numberOfAttributes;
    private int parameterPosition;
    private int parallelism;
    private StreamingRegression streamingRegression;
    private long lastScheduledTimestamp = -1;
    private long TIMER_DURATION = 100;
    private Scheduler scheduler;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        maxInstance = Integer.MAX_VALUE;
        batchSize = 1000;
        parallelism = 1;

        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                numberOfAttributes = ((Integer) attributeExpressionExecutors[0].execute(null));
                if (numberOfAttributes < 2) {
                    throw new ExecutionPlanValidationException("Number of attributes must be" +
                            " greater than 1");
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                        " first argument, required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[0].getReturnType().toString());
            }
        } else {
            throw new ExecutionPlanValidationException("Parameter count must be a constant and at" +
                    " least one configuration parameter required." +
                    " streamingRegressionSamoa(parCount,attribute_set) ");
        }

        int parameterWidth = attributeExpressionLength - numberOfAttributes;


        if (parameterWidth > 1) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the second argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[1].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Display interval  values" +
                        " must be a constant");
            }
        }

        if (parameterWidth > 2) {
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                    maxInstance = ((Integer) attributeExpressionExecutors[2].execute(null));
                    if (maxInstance == -1) {
                        maxInstance = Integer.MAX_VALUE;
                    }
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                            " third argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[0].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("The maximum number of events" +
                        " must be a constant");
            }
        }


        if (parameterWidth > 3) {
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                    parallelism = ((Integer) attributeExpressionExecutors[3].execute(null));
                } else {
                    throw new ExecutionPlanValidationException("Invalid parameter type found for" +
                            " the fourth argument, required " + Attribute.Type.INT + " but found " +
                            attributeExpressionExecutors[3].getReturnType().toString());
                }
            } else {
                throw new ExecutionPlanValidationException("Parallelism value must be a constant");
            }
        }

        parameterPosition = parameterWidth;
        lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime();
        streamingRegression = new StreamingRegression(maxInstance, batchSize, numberOfAttributes,
                parallelism);
        new Thread(streamingRegression).start();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(numberOfAttributes);
        for (int i = 0; i < numberOfAttributes - 1; i++) {
            attributes.add(new Attribute("att_" + i, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("prediction", Attribute.Type.DOUBLE));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk,
                           Processor nextProcessor, StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                if (complexEvent.getType() != ComplexEvent.Type.TIMER) {
                    double[] cepEvent = new double[attributeExpressionLength - parameterPosition];
                    Object evt;

                    Object classVal = attributeExpressionExecutors[attributeExpressionLength - 1].
                            execute(complexEvent);
                    double classValue = (double) classVal;

                    cepEvent[numberOfAttributes - 1] = classValue;
                    for (int i = 0; i < numberOfAttributes; i++) {
                        evt = attributeExpressionExecutors[i + parameterPosition].execute(complexEvent);
                        cepEvent[i] = (double) evt;
                    }

                    streamingRegression.addEvents(cepEvent);
                    Object[] outputData;
                    outputData = streamingRegression.getOutput();
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        StreamEvent streamEvent1 = new StreamEvent(0, 0,outputData.length);
                        streamEvent1.setOutputData(outputData);
                        complexEventChunk.add(streamEvent1);
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }

                } else {
                    lastScheduledTimestamp = lastScheduledTimestamp + TIMER_DURATION;
                    scheduler.notifyAt(lastScheduledTimestamp);

                    Object[] outputData;
                    outputData = streamingRegression.getOutput();
                    if (outputData == null) {
                        streamEventChunk.remove();
                    } else {
                        StreamEvent streamEvent1 = new StreamEvent(0, 0,outputData.length);
                        streamEvent1.setOutputData(outputData);
                        complexEventChunk.add(streamEvent1);
                        complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                    }
                }
            }
        }
        nextProcessor.process(complexEventChunk);
    }


    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[]{maxInstance, batchSize, numberOfAttributes, parameterPosition, parallelism,
                streamingRegression, lastScheduledTimestamp, TIMER_DURATION, scheduler};
    }

    @Override
    public void restoreState(Object[] state) {
        maxInstance = (int) state[0];
        batchSize = (int) state[1];
        numberOfAttributes = (int) state[2];
        parameterPosition = (int) state[3];
        parallelism = (int) state[4];
        streamingRegression = (StreamingRegression) state[5];
        lastScheduledTimestamp = (long) state[6];
        TIMER_DURATION = (long) state[7];
        scheduler = (Scheduler) state[8];
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp > 0) {
            lastScheduledTimestamp = executionPlanContext.getTimestampGenerator().currentTime() +
                    TIMER_DURATION;
            scheduler.notifyAt(lastScheduledTimestamp);
        }

    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }
}

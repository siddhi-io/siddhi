
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

import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.clustering.StreamingClustering;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class StreamingClusteringExtension extends StreamProcessor {

    private int numberOfAttributes;
    private int paramPosition;
    private int maxInstance;
    private int numberOfClusters;
    private StreamingClustering streamingClusteringWithSamoa;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        int parameterWidth;        // Number of configuring parameters
        maxInstance = Integer.MAX_VALUE;

        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                numberOfClusters = ((Integer) attributeExpressionExecutors[0].execute(null));
                if (numberOfClusters < 2) {
                    throw new ExecutionPlanValidationException("Number of clusters must be" +
                            " greater than 1");
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                        " first argument, required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[0].getReturnType().toString());
            }
        } else {
            throw new ExecutionPlanValidationException("Number of clusters must be" +
                    " a constant");
        }

        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                maxInstance = ((Integer) attributeExpressionExecutors[1].execute(null));
                if (maxInstance == -1) {
                    maxInstance = Integer.MAX_VALUE;
                }
            } else {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the" +
                        " second argument,required " + Attribute.Type.INT + " but found " +
                        attributeExpressionExecutors[1].getReturnType().toString());
            }
            parameterWidth = 2;
        } else {
            parameterWidth = 1;

        }
        numberOfAttributes = attributeExpressionLength - parameterWidth;
        paramPosition = parameterWidth;
        streamingClusteringWithSamoa = new StreamingClustering(maxInstance, numberOfAttributes,
                numberOfClusters);

        new Thread(streamingClusteringWithSamoa).start();

        // Add attributes
        List<Attribute> attributes = new ArrayList<Attribute>(numberOfClusters);
        for (int itr = 0; itr < numberOfClusters; itr++) {
            attributes.add(new Attribute(("center" + itr), Attribute.Type.STRING));
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                double[] cepEvent = new double[attributeExpressionLength - paramPosition];

                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    cepEvent[i - paramPosition] = (double) attributeExpressionExecutors[i].
                            execute(complexEvent);
                }

                streamingClusteringWithSamoa.addEvents(cepEvent);
                Object[] outputData;
                outputData = streamingClusteringWithSamoa.getOutput();

                // Skip processing if user has specified calculation interval
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{numberOfAttributes, paramPosition, maxInstance, numberOfClusters,
                streamingClusteringWithSamoa};
    }

    @Override
    public void restoreState(Object[] state) {
        numberOfAttributes = (int) state[0];
        paramPosition = (int) state[1];
        maxInstance = (int) state[2];
        numberOfClusters = (int) state[3];
        streamingClusteringWithSamoa = (StreamingClustering) state[4];
    }

}

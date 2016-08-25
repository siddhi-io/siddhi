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
package org.wso2.siddhi.extension.graph;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Operator which is related to find the maximum clique size of a graph.
 */
public class LargestConnectedComponentProcessor extends StreamProcessor {
    private VariableExpressionExecutor variableExpressionId;
    private VariableExpressionExecutor variableExpressionFriendId;
    private Graph graph = new Graph();
    private long largestConnectedComponentSize = 0;
    private boolean notifyUpdates;

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or
     *                              modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String id = (String) variableExpressionId.execute(event);
                String friendsId = (String) variableExpressionFriendId.execute(event);
                graph.addEdge(id, friendsId);
                long newLargestConnectedComponent = getLargestConnectedComponent();
                if (largestConnectedComponentSize != newLargestConnectedComponent) {
                    largestConnectedComponentSize = newLargestConnectedComponent;
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newLargestConnectedComponent});
                } else if (notifyUpdates) {
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newLargestConnectedComponent});
                } else {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    /**
     * The init method of the MaximumCliqueStreamProcessor,
     * this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 3) {
            throw new UnsupportedOperationException("Invalid no of arguments passed to graph:LargestConnectedComponentProcessor," + "required 3, but found" + attributeExpressionExecutors.length);
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the first parameter of graph:LargestConnectedComponentProcessor, Required a variable, but found a constant parameter  " + attributeExpressionExecutors[0].getReturnType());
            } else {
                variableExpressionId = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Invalid parameter found for the second parameter of graph:LargestConnectedComponentProcessor, Required a variable, but found a constant parameter " + attributeExpressionExecutors[1].getReturnType());
            } else {
                variableExpressionFriendId = (VariableExpressionExecutor) attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                    notifyUpdates = (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("LargestConnectedComponentProcessor's third parameter attribute should be a boolean value, but found " + attributeExpressionExecutors[0].getReturnType());
                }

            } else {
                throw new ExecutionPlanValidationException("LargestConnectedComponentProcessor should have constant parameter attribute but found a dynamic attribute " + attributeExpressionExecutors[2].getClass().getCanonicalName());
            }
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("largestConnectedComponent", Attribute.Type.LONG));
        return attributeList;
    }

    /**
     * Gets the number of vertices of the largest connected component of the graph
     *
     * @return the number of vertices in the largest connected component
     */
    public long getLargestConnectedComponent() {

        if (graph.size() == 0) {
            return 0L;
        }

        HashMap<String, Long> pegasusMap = new HashMap<String, Long>();

        long i = 0;
        for (String key : graph.getGraph().keySet()) {
            pegasusMap.put(key, i);
            i++;
        }
        boolean traversalPerformed;
        do {
            traversalPerformed = false;
            for (String userId : pegasusMap.keySet()) {
                for (String referUserId : pegasusMap.keySet()) {
                    if (graph.existsEdge(userId, referUserId)) {

                        if (pegasusMap.get(userId) > pegasusMap.get(referUserId)) {
                            pegasusMap.replace(userId, pegasusMap.get(referUserId));
                            traversalPerformed = true;
                        } else if (pegasusMap.get(userId) < pegasusMap.get(referUserId)) {
                            pegasusMap.replace(referUserId, pegasusMap.get(userId));
                            traversalPerformed = true;
                        }
                    }
                }
            }
        } while (traversalPerformed);
        return calculateLargestComponent(pegasusMap);
    }

    /**
     * Calculates the size largest connected component
     *
     * @param pegasusMap is the reference to the pegasusmap
     * @return size of largest connected component
     */
    private long calculateLargestComponent(HashMap<String, Long> pegasusMap) {
        long largestComponent = 0;
        for (Long nodeId : pegasusMap.values()) {
            int count = 0;
            for (Long referNodeId : pegasusMap.values()) {
                if (nodeId.equals(referNodeId)) {
                    count++;
                }
            }
            if (count > largestComponent) {
                largestComponent = count;
            }
        }
        return largestComponent;
    }

    /**
     * This will be called only once and this can be used to acquire required resources for the
     * processing element.
     * This will be called after initializing the system and before starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release the acquired resources
     * for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state at a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on the same order provided
     *              by currentState().
     */
    @Override
    public void restoreState(Object[] state) {

    }
}

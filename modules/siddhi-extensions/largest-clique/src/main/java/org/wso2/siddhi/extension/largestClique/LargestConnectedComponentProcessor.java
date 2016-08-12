/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.extension.largestClique;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by bhagya on 8/10/16.
 */
public class LargestConnectedComponentProcessor extends StreamProcessor {
    private VariableExpressionExecutor variableExpressionID;
    private VariableExpressionExecutor variableExpressionFriendID;
    private Graph graph = new Graph();
    private long largestConnectedComponentSize = 0;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                Long id = (Long) variableExpressionID.execute(event);
                Long friendsId = (Long) variableExpressionFriendID.execute(event);
                graph.addVertex(id);
                graph.addVertex(friendsId);
                graph.addEdge(id, friendsId);
                long newLargestConnectedComponent = getLargestConnectedComponent();
                if (largestConnectedComponentSize != newLargestConnectedComponent) {
                    largestConnectedComponentSize = newLargestConnectedComponent;
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newLargestConnectedComponent});
                } else {
                    streamEventChunk.remove();
                }
            }

        }

        nextProcessor.process(streamEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 2)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        } else {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Required a variable, but found a other parameter");
            } else {
                variableExpressionID = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            }
            if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
                throw new UnsupportedOperationException("Required a variable, but found a other parameter");
            } else {
                variableExpressionFriendID = (VariableExpressionExecutor) attributeExpressionExecutors[1];
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

        HashMap<Long, Long> pegasusMap = new HashMap<Long, Long>();

        long i = 0;
        for (Long key : graph.getGraph().keySet()) {
            pegasusMap.put(key, i);
            i++;
        }
        boolean changes;
        do {
            changes = false;
            for (Long userId : pegasusMap.keySet()) {
                for (Long referUserId : pegasusMap.keySet()) {
                    if (graph.existsEdge(userId, referUserId)) {

                        if (pegasusMap.get(userId) > pegasusMap.get(referUserId)) {
                            pegasusMap.replace(userId, pegasusMap.get(referUserId));
                            changes = true;
                        } else if (pegasusMap.get(userId) < pegasusMap.get(referUserId)) {
                            pegasusMap.replace(referUserId, pegasusMap.get(userId));
                            changes = true;
                        }
                    }
                }
            }
        } while (changes);
        return calculateLargestComponent(pegasusMap);
    }
    /**
     * Calculates the size largest connected component
     * @param pegasusMap is the reference to the pegasusmap
     * @return size of largest connected component
     */
    private long calculateLargestComponent(HashMap<Long, Long> pegasusMap) {
        long largeComponent = 0;
        for (Long nodeId : pegasusMap.values()) {
            int count = 0;
            for (Long referNodeId : pegasusMap.values()) {
                if (nodeId.equals(referNodeId)) {
                    count++;
                }
            }
            if (count > largeComponent) {
                largeComponent = count;
            }
        }
        return largeComponent;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}

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

import java.util.*;

/**
 * Created on 8/5/16.
 */
public class LargestCliqueStreamProcessor extends StreamProcessor {
    private VariableExpressionExecutor variableExpressionID;
    private VariableExpressionExecutor variableExpressionFriendID;
    private Graph graph = new Graph();
    private int maxClique = 0;

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
                int newMaxClique = maxClique(id, maxClique);
                if (maxClique != newMaxClique) {
                    maxClique = newMaxClique;
                    complexEventPopulater.populateComplexEvent(event, new Object[]{newMaxClique});
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
        attributeList.add(new Attribute("largestClique", Attribute.Type.INT));
        return attributeList;
    }

    public int maxClique(long vertexId, int minClique) {
        HashSet<Long> clique = new HashSet<Long>();
        clique.add(vertexId);
        Set<Long> neighbors = graph.getNeighbors(vertexId);
        Set<Long> copyNeighbors = copy(neighbors);
        Iterator<Long> iterator = copyNeighbors.iterator();
        /*
         * Prune candidates: vertices with smaller degree
		 * than minClique can never be in the max clique
		 */
        while (iterator.hasNext()) {
            Long candidate = iterator.next();
            if (graph.getDegree(candidate) < minClique) {
                iterator.remove();
            }
        }
        int maxClique = maxClique(clique, neighbors, minClique);
        return maxClique;
    }

    private int maxClique(Set<Long> clique, Set<Long> candidates, int minClique) {
        Iterator<Long> iterator = candidates.iterator();
        // pruning: filter candidates _ all candidates must have at least local degree minClique into the clique
        // vertices, otherwise they could never participate in a larger clique as minClique
        while (iterator.hasNext()) {
            Long candidate = iterator.next();
            if (getLocalDegree(candidate, clique) < clique.size()) {
                iterator.remove();
            }
        }
        // pruning2: if clique size + candidates size smaller minClique, we can never achieve minClique
        if (clique.size() + candidates.size() < minClique) {
            return minClique;
        }
        int max = clique.size();
        for (Long candidate : candidates) {
            if (isValidCliqueMember(candidate, clique)) {

                Set<Long> newClique = copy(clique);
                newClique.add(candidate);

                Set<Long> newCandidates = copy(candidates);
                newCandidates.remove(candidate);

                int temp = maxClique(newClique, newCandidates, Math.max(minClique, max));
                if (temp > max) {
                    max = temp;
                }
            }
        }
        return max;
    }

    /**
     * get the degree of the vertex
     *
     * @param userId
     * @param clique
     * @return
     */
    private int getLocalDegree(long userId, Set<Long> clique) {
        int count = 0;
        for (Long cliqueMember : clique) {
            if (graph.existsEdge(userId, cliqueMember)) {
                count++;
            }
        }
        return count;
    }

    /**
     * return true if the vertex exists in the clique
     *
     * @param candidate
     * @param clique
     * @return
     */
    private boolean isValidCliqueMember(Long candidate, Set<Long> clique) {
        for (Long cliqueVertex : clique) {
            if (!graph.existsEdge(candidate, cliqueVertex)) {
                return false;
            }
        }
        return true;
    }

    public static Set<Long> copy(Set<Long> l) {
        Set<Long> l2 = new HashSet<Long>();
        for (Long lo : l) {
            l2.add(lo);
        }
        return l2;
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

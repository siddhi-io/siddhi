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

package org.wso2.siddhi.core.query.output.ratelimit.snapshot;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.GroupedComplexEvent;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

public class AggregationGroupByWindowedPerSnapshotOutputRateLimiter extends AggregationWindowedPerSnapshotOutputRateLimiter {
    private Map<String, Map<Integer, Object>> groupByAggregateAttributeValueMap;
    protected LinkedList<GroupedComplexEvent> eventList;
    protected String queryName;

    protected AggregationGroupByWindowedPerSnapshotOutputRateLimiter(String id, Long value, ScheduledExecutorService scheduledExecutorService, List<Integer> aggregateAttributePositionList, WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter, ExecutionPlanContext executionPlanContext,String queryName) {
        super(id, value, scheduledExecutorService, aggregateAttributePositionList, wrappedSnapshotOutputRateLimiter, executionPlanContext,queryName);
        this.queryName=queryName;
        groupByAggregateAttributeValueMap = new HashMap<String, Map<Integer, Object>>();
        eventList = new LinkedList<GroupedComplexEvent>();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        List<ComplexEventChunk<ComplexEvent>> outputEventChunks = new ArrayList<ComplexEventChunk<ComplexEvent>>();
        synchronized (this) {
            complexEventChunk.reset();
            String currentGroupByKey = null;
            Map<Integer, Object> currentAggregateAttributeValueMap = null;
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                if (event.getType() == ComplexEvent.Type.TIMER) {
                    tryFlushEvents(outputEventChunks, event);
                } else {
                    complexEventChunk.remove();
                    tryFlushEvents(outputEventChunks, event);
                    GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                    if (currentGroupByKey == null || !currentGroupByKey.equals(groupedComplexEvent.getGroupKey())) {
                        currentGroupByKey = groupedComplexEvent.getGroupKey();
                        currentAggregateAttributeValueMap = groupByAggregateAttributeValueMap.get(currentGroupByKey);
                        if (currentAggregateAttributeValueMap == null) {
                            currentAggregateAttributeValueMap = new HashMap<Integer, Object>(aggregateAttributePositionList.size());
                            groupByAggregateAttributeValueMap.put(currentGroupByKey, currentAggregateAttributeValueMap);
                        }
                    }
                    if (groupedComplexEvent.getType() == ComplexEvent.Type.CURRENT) {
                        eventList.add(groupedComplexEvent);
                        for (Integer position : aggregateAttributePositionList) {
                            currentAggregateAttributeValueMap.put(position, event.getOutputData()[position]);
                        }
                    } else if (groupedComplexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                        for (Iterator<GroupedComplexEvent> iterator = eventList.iterator(); iterator.hasNext(); ) {
                            GroupedComplexEvent currentEvent = iterator.next();
                            if (comparator.compare(currentEvent.getComplexEvent(), groupedComplexEvent.getComplexEvent()) == 0) {
                                iterator.remove();
                                for (Integer position : aggregateAttributePositionList) {
                                    currentAggregateAttributeValueMap.put(position, groupedComplexEvent.getOutputData()[position]);
                                }
                                break;
                            }
                        }
                    }else if (groupedComplexEvent.getType() == ComplexEvent.Type.RESET) {
                        eventList.clear();
                        groupByAggregateAttributeValueMap.clear();
                    }
                }
            }
        }
        for (ComplexEventChunk eventChunk : outputEventChunks) {
            sendToCallBacks(eventChunk);
        }
    }

    private void tryFlushEvents(List<ComplexEventChunk<ComplexEvent>> outputEventChunks, ComplexEvent event) {
        if (event.getTimestamp() >= scheduledTime) {
            constructOutputChunk(outputEventChunks);
            scheduledTime = scheduledTime + value;
            scheduler.notifyAt(scheduledTime);
        }
    }

    private void constructOutputChunk(List<ComplexEventChunk<ComplexEvent>> outputEventChunks) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<ComplexEvent>(false);
        for (GroupedComplexEvent originalComplexEvent : eventList) {
            String currentGroupByKey = originalComplexEvent.getGroupKey();
            Map<Integer, Object> currentAggregateAttributeValueMap = groupByAggregateAttributeValueMap.get(currentGroupByKey);
            ComplexEvent eventCopy = cloneComplexEvent(originalComplexEvent.getComplexEvent());
            for (Integer position : aggregateAttributePositionList) {
                eventCopy.getOutputData()[position] = currentAggregateAttributeValueMap.get(position);
            }
            outputEventChunk.add(eventCopy);
        }
        outputEventChunks.add(outputEventChunk);
    }

    @Override
    public Object[] currentState() {
        return new Object[]{eventList, groupByAggregateAttributeValueMap};
    }

    @Override
    public void restoreState(Object[] state) {
        eventList = (LinkedList<GroupedComplexEvent>) state[0];
        groupByAggregateAttributeValueMap = (Map<String, Map<Integer, Object>>) state[1];
    }

    @Override
    public SnapshotOutputRateLimiter clone(String key, WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter) {
        return new AggregationGroupByWindowedPerSnapshotOutputRateLimiter(id + key, value, scheduledExecutorService, aggregateAttributePositionList, wrappedSnapshotOutputRateLimiter, executionPlanContext,queryName);
    }
}

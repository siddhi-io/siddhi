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

package io.siddhi.core.query.output.ratelimit.snapshot;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.GroupedComplexEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.snapshot.state.StateFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link PerSnapshotOutputRateLimiter} for queries with GroupBy, Aggregators and Windows.
 */
public class AggregationGroupByWindowedPerSnapshotOutputRateLimiter extends
        AggregationWindowedPerSnapshotOutputRateLimiter {

    protected AggregationGroupByWindowedPerSnapshotOutputRateLimiter(Long value,
                                                                     List<Integer> aggregateAttributePositionList,
                                                                     WrappedSnapshotOutputRateLimiter
                                                                             wrappedSnapshotOutputRateLimiter,
                                                                     boolean groupBy,
                                                                     SiddhiQueryContext siddhiQueryContext) {
        super(value, aggregateAttributePositionList, wrappedSnapshotOutputRateLimiter,
                groupBy, siddhiQueryContext);
    }

    @Override
    protected StateFactory<AggregationRateLimiterState> init() {
        this.scheduler = SchedulerParser.parse(this, siddhiQueryContext);
        this.scheduler.setStreamEventFactory(new StreamEventFactory(0, 0, 0));
        this.scheduler.init(lockWrapper, siddhiQueryContext.getName());
        return () -> new AggregationGroupByRateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        List<ComplexEventChunk> outputEventChunks = new LinkedList<>();
        AggregationGroupByRateLimiterState state = (AggregationGroupByRateLimiterState) stateHolder.getState();
        try {
            synchronized (state) {
                complexEventChunk.reset();
                String currentGroupByKey = null;
                Map<Integer, Object> currentAggregateAttributeValueMap = null;
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.TIMER) {
                        tryFlushEvents(outputEventChunks, event, state);
                    } else {
                        complexEventChunk.remove();
                        tryFlushEvents(outputEventChunks, event, state);
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        if (currentGroupByKey == null || !currentGroupByKey.equals(groupedComplexEvent.getGroupKey())) {
                            currentGroupByKey = groupedComplexEvent.getGroupKey();
                            currentAggregateAttributeValueMap =
                                    state.groupByAggregateAttributeValueMap.get(currentGroupByKey);
                            if (currentAggregateAttributeValueMap == null) {
                                currentAggregateAttributeValueMap = new HashMap<Integer, Object>
                                        (aggregateAttributePositionList.size());
                                state.groupByAggregateAttributeValueMap.put(currentGroupByKey,
                                        currentAggregateAttributeValueMap);
                            }
                        }
                        if (groupedComplexEvent.getType() == ComplexEvent.Type.CURRENT) {
                            state.eventList.add(groupedComplexEvent);
                            for (Integer position : aggregateAttributePositionList) {
                                currentAggregateAttributeValueMap.put(position, event.getOutputData()[position]);
                            }
                        } else if (groupedComplexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                            for (Iterator<GroupedComplexEvent> iterator = state.eventList.iterator();
                                 iterator.hasNext(); ) {
                                GroupedComplexEvent currentEvent = iterator.next();
                                if (comparator.compare(currentEvent.getComplexEvent(), groupedComplexEvent
                                        .getComplexEvent()) == 0) {
                                    iterator.remove();
                                    for (Integer position : aggregateAttributePositionList) {
                                        currentAggregateAttributeValueMap.put(position,
                                                groupedComplexEvent.getOutputData()[position]);
                                    }
                                    break;
                                }
                            }
                        } else if (groupedComplexEvent.getType() == ComplexEvent.Type.RESET) {
                            state.eventList.clear();
                            state.groupByAggregateAttributeValueMap.clear();
                        }
                    }
                }
            }
        } finally {
            stateHolder.returnState(state);
        }
        sendToCallBacks(outputEventChunks);
    }

    private void tryFlushEvents(List<ComplexEventChunk> outputEventChunks,
                                ComplexEvent event,
                                AggregationGroupByRateLimiterState state) {
        if (event.getTimestamp() >= state.scheduledTime) {
            constructOutputChunk(outputEventChunks, state);
            state.scheduledTime = state.scheduledTime + value;
            scheduler.notifyAt(state.scheduledTime);
        }
    }

    private void constructOutputChunk(List<ComplexEventChunk> outputEventChunks,
                                      AggregationGroupByRateLimiterState state) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
        Set<String> outputGroupingKeys = new HashSet<>();
        for (GroupedComplexEvent originalComplexEvent : state.eventList) {
            String currentGroupByKey = originalComplexEvent.getGroupKey();
            if (!outputGroupingKeys.contains(currentGroupByKey)) {
                outputGroupingKeys.add(currentGroupByKey);
                Map<Integer, Object> currentAggregateAttributeValueMap = state.groupByAggregateAttributeValueMap.get
                        (currentGroupByKey);
                ComplexEvent eventCopy = cloneComplexEvent(originalComplexEvent.getComplexEvent());
                for (Integer position : aggregateAttributePositionList) {
                    eventCopy.getOutputData()[position] = currentAggregateAttributeValueMap.get(position);
                }
                outputEventChunk.add(eventCopy);
            }
        }
        outputEventChunks.add(outputEventChunk);
    }

    class AggregationGroupByRateLimiterState extends AggregationRateLimiterState {

        private List<GroupedComplexEvent> eventList;
        private Map<String, Map<Integer, Object>> groupByAggregateAttributeValueMap;

        public AggregationGroupByRateLimiterState() {
            groupByAggregateAttributeValueMap = new HashMap<>();
            eventList = new LinkedList<>();
        }

        @Override
        public boolean canDestroy() {
            return groupByAggregateAttributeValueMap.isEmpty() && eventList.isEmpty() && scheduledTime == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("EventList", eventList);
            state.put("GroupByAggregateAttributeValueMap", groupByAggregateAttributeValueMap);
            state.put("ScheduledTime", scheduledTime);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            eventList = (List<GroupedComplexEvent>) state.get("EventList");
            groupByAggregateAttributeValueMap = (Map<String, Map<Integer, Object>>) state.get
                    ("GroupByAggregateAttributeValueMap");
            scheduledTime = (Long) state.get("ScheduledTime");
        }
    }
}

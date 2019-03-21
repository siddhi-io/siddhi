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
import io.siddhi.core.event.stream.StreamEventPool;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link PerSnapshotOutputRateLimiter} for queries with Windows and Aggregators.
 */
public class AggregationWindowedPerSnapshotOutputRateLimiter
        extends SnapshotOutputRateLimiter
        <AggregationWindowedPerSnapshotOutputRateLimiter.AggregationRateLimiterState> {
    protected final Long value;
    protected Comparator<ComplexEvent> comparator;
    protected List<Integer> aggregateAttributePositionList;
    protected Scheduler scheduler;
    protected long scheduledTime;


    protected AggregationWindowedPerSnapshotOutputRateLimiter(Long value,
                                                              final List<Integer> aggregateAttributePositionList,
                                                              WrappedSnapshotOutputRateLimiter
                                                                      wrappedSnapshotOutputRateLimiter,
                                                              boolean groupBy, SiddhiQueryContext siddhiQueryContext) {
        super(wrappedSnapshotOutputRateLimiter, siddhiQueryContext, groupBy);
        this.value = value;
        this.aggregateAttributePositionList = aggregateAttributePositionList;
        Collections.sort(aggregateAttributePositionList);
        this.comparator = new Comparator<ComplexEvent>() {
            Integer[] aggregateAttributePositions = aggregateAttributePositionList.toArray(new
                    Integer[aggregateAttributePositionList.size()]);
            int ignoreIndexLength = aggregateAttributePositions.length;

            @Override
            public int compare(ComplexEvent event1, ComplexEvent event2) {
                int ignoreIndex = 0;
                int ignoreIndexPosition = aggregateAttributePositions[0];
                Object[] data = event1.getOutputData();
                for (int i = 0; i < data.length; i++) {
                    if (ignoreIndexPosition == i) {
                        ignoreIndex++;
                        if (ignoreIndex == ignoreIndexLength) {
                            ignoreIndexPosition = -1;
                        } else {
                            ignoreIndexPosition = aggregateAttributePositions[i];
                        }
                        continue;
                    }
                    if (!data[i].equals(event2.getOutputData()[i])) {
                        return 1;
                    }

                }
                return 0;
            }
        };
    }

    @Override
    protected StateFactory<AggregationRateLimiterState> init() {
        return () -> new AggregationRateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        ArrayList<ComplexEventChunk<ComplexEvent>> outputEventChunks = new ArrayList<ComplexEventChunk<ComplexEvent>>();
        AggregationRateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                if (event.getType() == ComplexEvent.Type.TIMER) {
                    tryFlushEvents(outputEventChunks, event, state);
                } else {
                    complexEventChunk.remove();
                    tryFlushEvents(outputEventChunks, event, state);
                    if (event.getType() == ComplexEvent.Type.CURRENT) {
                        state.eventList.add(event);
                        for (Integer position : aggregateAttributePositionList) {
                            state.aggregateAttributeValueMap.put(position, event.getOutputData()[position]);
                        }
                    } else if (event.getType() == ComplexEvent.Type.EXPIRED) {
                        for (Iterator<ComplexEvent> iterator = state.eventList.iterator(); iterator.hasNext(); ) {
                            ComplexEvent complexEvent = iterator.next();
                            if (comparator.compare(event, complexEvent) == 0) {
                                iterator.remove();
                                for (Integer position : aggregateAttributePositionList) {
                                    state.aggregateAttributeValueMap.put(position, event.getOutputData()[position]);
                                }
                                break;
                            }
                        }
                    } else if (event.getType() == ComplexEvent.Type.RESET) {
                        state.eventList.clear();
                        state.aggregateAttributeValueMap.clear();
                    }
                }
            }
        }
        } finally {
            stateHolder.returnState(state);
        }
        for (ComplexEventChunk eventChunk : outputEventChunks) {
            sendToCallBacks(eventChunk);
        }
    }

    private void tryFlushEvents(ArrayList<ComplexEventChunk<ComplexEvent>> outputEventChunks, ComplexEvent event,
                                AggregationRateLimiterState state) {
        if (event.getTimestamp() >= scheduledTime) {
            ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<ComplexEvent>(false);
            for (ComplexEvent originalComplexEvent : state.eventList) {
                ComplexEvent eventCopy = cloneComplexEvent(originalComplexEvent);
                for (Integer position : aggregateAttributePositionList) {
                    eventCopy.getOutputData()[position] = state.aggregateAttributeValueMap.get(position);
                }
                outputEventChunk.add(eventCopy);
            }
            outputEventChunks.add(outputEventChunk);
            scheduledTime += value;
            scheduler.notifyAt(scheduledTime);
        }
    }

    @Override
    public void start() {
        scheduler = SchedulerParser.parse(this, siddhiQueryContext);
        scheduler.setStreamEventPool(new StreamEventPool(0, 0, 0, 5));
        scheduler.init(lockWrapper, siddhiQueryContext.getName());
        long currentTime = System.currentTimeMillis();
        scheduledTime = currentTime + value;
        scheduler.notifyAt(scheduledTime);
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    class AggregationRateLimiterState extends State {

        private List<ComplexEvent> eventList;
        private Map<Integer, Object> aggregateAttributeValueMap;

        public AggregationRateLimiterState() {
            this.eventList = new LinkedList<ComplexEvent>();
            aggregateAttributeValueMap = new HashMap<Integer, Object>(aggregateAttributePositionList.size());
        }

        @Override
        public boolean canDestroy() {
            return aggregateAttributeValueMap.isEmpty() && eventList.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("EventList", eventList);
                state.put("AggregateAttributeValueMap", aggregateAttributeValueMap);
            }
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            eventList = (List<ComplexEvent>) state.get("EventList");
            aggregateAttributeValueMap = (Map<Integer, Object>) state.get("AdgregateAttributeValueMap");
        }
    }
}

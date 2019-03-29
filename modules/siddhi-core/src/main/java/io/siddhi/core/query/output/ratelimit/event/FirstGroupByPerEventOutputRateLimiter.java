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

package io.siddhi.core.query.output.ratelimit.event;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.GroupedComplexEvent;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined number of events and the emit only the
 * first event. This implementation specifically handle queries with group by.
 */
public class FirstGroupByPerEventOutputRateLimiter
        extends OutputRateLimiter<FirstGroupByPerEventOutputRateLimiter.RateLimiterState> {
    private final Integer value;

    public FirstGroupByPerEventOutputRateLimiter(String id, Integer value) {
        this.value = value;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        return () -> new RateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        ArrayList<ComplexEventChunk<ComplexEvent>> outputEventChunks = new ArrayList<ComplexEventChunk<ComplexEvent>>();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.CURRENT || event.getType() == ComplexEvent.Type.EXPIRED) {
                        complexEventChunk.remove();
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        if (!state.groupByKeys.contains(groupedComplexEvent.getGroupKey())) {
                            state.groupByKeys.add(groupedComplexEvent.getGroupKey());
                            state.allComplexEventChunk.add(groupedComplexEvent.getComplexEvent());
                        }
                        if (++state.counter == value) {
                            if (state.allComplexEventChunk.getFirst() != null) {
                                ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<ComplexEvent>
                                        (complexEventChunk.isBatch());
                                outputEventChunk.add(state.allComplexEventChunk.getFirst());
                                outputEventChunks.add(outputEventChunk);
                                state.allComplexEventChunk.clear();
                                state.counter = 0;
                                state.groupByKeys.clear();
                            } else {
                                state.counter = 0;
                                state.groupByKeys.clear();
                            }

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

    @Override
    public void partitionCreated() {
        //Nothing to be done
    }

    class RateLimiterState extends State {
        private List<String> groupByKeys = new ArrayList<String>();
        private ComplexEventChunk<ComplexEvent> allComplexEventChunk = new ComplexEventChunk<ComplexEvent>(false);

        private volatile int counter = 0;

        @Override
        public boolean canDestroy() {
            return counter == 0 && groupByKeys.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Counter", counter);
            state.put("GroupByKeys", groupByKeys);
            state.put("AllComplexEventChunk", allComplexEventChunk.getFirst());
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            counter = (int) state.get("Counter");
            groupByKeys = (List<String>) state.get("GroupByKeys");
            allComplexEventChunk.clear();
            allComplexEventChunk.add((ComplexEvent) state.get("AllComplexEventChunk"));
        }
    }

}

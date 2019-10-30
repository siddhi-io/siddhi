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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined number of events and the emit only the
 * last event. This implementation specifically handle queries with group by.
 */
public class LastGroupByPerEventOutputRateLimiter extends
        OutputRateLimiter<LastGroupByPerEventOutputRateLimiter.RateLimiterState> {
    private final Integer value;

    public LastGroupByPerEventOutputRateLimiter(String id, Integer value) {
        this.value = value;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        return () -> new RateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.CURRENT || event.getType() == ComplexEvent.Type.EXPIRED) {
                        complexEventChunk.remove();
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        state.allGroupByKeyEvents.put(groupedComplexEvent.getGroupKey(),
                                groupedComplexEvent.getComplexEvent());
                        if (++state.counter == value) {
                            state.counter = 0;
                            if (state.allGroupByKeyEvents.size() != 0) {
                                for (ComplexEvent complexEvent : state.allGroupByKeyEvents.values()) {
                                    outputEventChunk.add(complexEvent);
                                }
                                state.allGroupByKeyEvents.clear();
                            }
                        }
                    }
                }
            }
        } finally {
            stateHolder.returnState(state);
        }
        outputEventChunk.reset();
        if (outputEventChunk.hasNext()) {
            sendToCallBacks(outputEventChunk);
        }
    }

    @Override
    public void partitionCreated() {
        //Nothing to be done
    }

    class RateLimiterState extends State {

        private volatile int counter = 0;
        private Map<String, ComplexEvent> allGroupByKeyEvents = new LinkedHashMap<String, ComplexEvent>();

        @Override
        public boolean canDestroy() {
            return counter == 0 && allGroupByKeyEvents.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Counter", counter);
            state.put("AllGroupByKeyEvents", allGroupByKeyEvents);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            counter = (int) state.get("Counter");
            allGroupByKeyEvents = (Map<String, ComplexEvent>) state.get("AllGroupByKeyEvents");
        }
    }
}

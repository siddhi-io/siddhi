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
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    complexEventChunk.remove();
                    GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                    Integer count = state.groupByOutputTime.get(groupedComplexEvent.getGroupKey());
                    if (count == null) {
                        state.groupByOutputTime.put(groupedComplexEvent.getGroupKey(), 1);
                        outputEventChunk.add(groupedComplexEvent);
                    } else if (count.equals(value - 1)) {
                        state.groupByOutputTime.remove(groupedComplexEvent.getGroupKey());
                    } else {
                        state.groupByOutputTime.put(groupedComplexEvent.getGroupKey(), count + 1);
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
        private Map<String, Integer> groupByOutputTime = new HashMap();

        @Override
        public boolean canDestroy() {
            return groupByOutputTime.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("GroupByOutputTime", groupByOutputTime);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            groupByOutputTime = (Map<String, Integer>) state.get("GroupByOutputTime");
        }
    }

}

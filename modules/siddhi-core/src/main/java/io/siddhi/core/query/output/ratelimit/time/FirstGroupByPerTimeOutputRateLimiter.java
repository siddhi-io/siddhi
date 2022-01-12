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

package io.siddhi.core.query.output.ratelimit.time;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.GroupedComplexEvent;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined time period and the emit only first
 * event. This implementation specifically represent GroupBy queries.
 */
public class FirstGroupByPerTimeOutputRateLimiter
        extends OutputRateLimiter<FirstGroupByPerTimeOutputRateLimiter.RateLimiterState> implements Schedulable {
    private static final Logger log = LogManager.getLogger(FirstGroupByPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;

    public FirstGroupByPerTimeOutputRateLimiter(String id, Long value) {
        this.id = id;
        this.value = value;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        return () -> new RateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
        complexEventChunk.reset();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                long currentTime = siddhiQueryContext.getSiddhiAppContext().
                        getTimestampGenerator().currentTime();
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    complexEventChunk.remove();
                    GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                    Long outputTime = state.groupByOutputTime.get(groupedComplexEvent.getGroupKey());
                    if (outputTime == null || outputTime + value <= currentTime) {
                        state.groupByOutputTime.put(groupedComplexEvent.getGroupKey(), currentTime);
                        outputEventChunk.add(groupedComplexEvent);
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

    }

    class RateLimiterState extends State {

        private Map<String, Long> groupByOutputTime = new HashMap();

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
            groupByOutputTime = (Map<String, Long>) state.get("GroupByOutputTime");
        }
    }
}

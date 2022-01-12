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
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined time period and the emit only last
 * event. This implementation specifically represent GroupBy queries.
 */
public class LastGroupByPerTimeOutputRateLimiter
        extends OutputRateLimiter<LastGroupByPerTimeOutputRateLimiter.RateLimiterState> implements Schedulable {
    private static final Logger log = LogManager.getLogger(LastGroupByPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;
    private Scheduler scheduler;

    public LastGroupByPerTimeOutputRateLimiter(String id, Long value) {
        this.id = id;
        this.value = value;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        this.scheduler = SchedulerParser.parse(this, siddhiQueryContext);
        this.scheduler.setStreamEventFactory(new StreamEventFactory(0, 0, 0));
        this.scheduler.init(lockWrapper, siddhiQueryContext.getName());
        return () -> new RateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
        complexEventChunk.reset();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.TIMER) {
                        if (event.getTimestamp() >= state.scheduledTime) {
                            if (state.allGroupByKeyEvents.size() != 0) {
                                for (ComplexEvent complexEvent : state.allGroupByKeyEvents.values()) {
                                    outputEventChunk.add(complexEvent);
                                }
                                state.allGroupByKeyEvents.clear();
                            }
                            state.scheduledTime = state.scheduledTime + value;
                            scheduler.notifyAt(state.scheduledTime);
                        }
                    } else if (event.getType() == ComplexEvent.Type.CURRENT || event.getType() == ComplexEvent.Type
                            .EXPIRED) {
                        complexEventChunk.remove();
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        state.allGroupByKeyEvents.put(groupedComplexEvent.getGroupKey(),
                                groupedComplexEvent.getComplexEvent());
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
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                long currentTime = System.currentTimeMillis();
                state.scheduledTime = currentTime + value;
                scheduler.notifyAt(state.scheduledTime);
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    class RateLimiterState extends State {

        public long scheduledTime;
        private Map<String, ComplexEvent> allGroupByKeyEvents = new LinkedHashMap<String, ComplexEvent>();

        @Override
        public boolean canDestroy() {
            return allGroupByKeyEvents.size() == 0 && scheduledTime == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("AllGroupByKeyEvents", allGroupByKeyEvents);
            state.put("ScheduledTime", scheduledTime);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            allGroupByKeyEvents = (Map<String, ComplexEvent>) state.get("AllGroupByKeyEvents");
            scheduledTime = (Long) state.get("ScheduledTime");
        }
    }

}

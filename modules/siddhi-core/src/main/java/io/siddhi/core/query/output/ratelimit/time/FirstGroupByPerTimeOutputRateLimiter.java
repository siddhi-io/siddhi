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
import io.siddhi.core.event.stream.StreamEventPool;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined time period and the emit only first
 * event. This implementation specifically represent GroupBy queries.
 */
public class FirstGroupByPerTimeOutputRateLimiter
        extends OutputRateLimiter<FirstGroupByPerTimeOutputRateLimiter.RateLimiterState> implements Schedulable {
    private static final Logger log = Logger.getLogger(FirstGroupByPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;
    private ScheduledExecutorService scheduledExecutorService;
    private Scheduler scheduler;
    private long scheduledTime;

    public FirstGroupByPerTimeOutputRateLimiter(String id, Long value, ScheduledExecutorService
            scheduledExecutorService) {
        this.id = id;
        this.value = value;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        return () -> new RateLimiterState();
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ArrayList<ComplexEventChunk<ComplexEvent>> outputEventChunks = new ArrayList<ComplexEventChunk<ComplexEvent>>();
        complexEventChunk.reset();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.TIMER) {
                        if (event.getTimestamp() >= scheduledTime) {
                            if (state.allComplexEventChunk.getFirst() != null) {
                                ComplexEventChunk<ComplexEvent> eventChunk = new ComplexEventChunk<ComplexEvent>
                                        (complexEventChunk.isBatch());
                                eventChunk.add(state.allComplexEventChunk.getFirst());
                                state.allComplexEventChunk.clear();
                                state.groupByKeys.clear();
                                outputEventChunks.add(eventChunk);
                            } else {
                                state.groupByKeys.clear();
                            }
                            scheduledTime = scheduledTime + value;
                            scheduler.notifyAt(scheduledTime);
                        }
                    } else if (event.getType() == ComplexEvent.Type.CURRENT || event.getType() == ComplexEvent.Type
                            .EXPIRED) {
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        if (!state.groupByKeys.contains(groupedComplexEvent.getGroupKey())) {
                            complexEventChunk.remove();
                            state.groupByKeys.add(groupedComplexEvent.getGroupKey());
                            state.allComplexEventChunk.add(groupedComplexEvent.getComplexEvent());
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

    class RateLimiterState extends State {

        private List<String> groupByKeys = new ArrayList<String>();
        private ComplexEventChunk<ComplexEvent> allComplexEventChunk =
                new ComplexEventChunk<ComplexEvent>(false);


        @Override
        public boolean canDestroy() {
            return groupByKeys.size() == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("AllComplexEventChunk", allComplexEventChunk.getFirst());
                state.put("GroupByKeys", groupByKeys);
            }
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            allComplexEventChunk.clear();
            allComplexEventChunk.add((ComplexEvent) state.get("AllComplexEventChunk"));
            groupByKeys = (List<String>) state.get("GroupByKeys");
        }
    }
}

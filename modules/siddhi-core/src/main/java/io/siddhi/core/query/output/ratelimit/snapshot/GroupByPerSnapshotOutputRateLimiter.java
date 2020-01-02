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
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link PerSnapshotOutputRateLimiter} for queries with GroupBy.
 */
public class GroupByPerSnapshotOutputRateLimiter
        extends SnapshotOutputRateLimiter<GroupByPerSnapshotOutputRateLimiter.RateLimiterState> {
    private final Long value;
    private Scheduler scheduler;

    public GroupByPerSnapshotOutputRateLimiter(Long value,
                                               WrappedSnapshotOutputRateLimiter wrappedSnapshotOutputRateLimiter,
                                               boolean groupBy, SiddhiQueryContext siddhiQueryContext) {
        super(wrappedSnapshotOutputRateLimiter, siddhiQueryContext, groupBy);
        this.value = value;
    }

    @Override
    protected StateFactory<RateLimiterState> init() {
        this.scheduler = SchedulerParser.parse(this, siddhiQueryContext);
        this.scheduler.setStreamEventFactory(new StreamEventFactory(0, 0, 0));
        this.scheduler.init(lockWrapper, siddhiQueryContext.getName());
        return () -> new RateLimiterState();
    }

    /**
     * Sends the collected unique outputs per group by key upon arrival of timer event from scheduler.
     *
     * @param complexEventChunk Incoming {@link ComplexEventChunk}
     */
    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        List<ComplexEventChunk> outputEventChunks = new LinkedList<>();
        complexEventChunk.reset();
        RateLimiterState state = stateHolder.getState();
        try {
            synchronized (state) {
                complexEventChunk.reset();
                while (complexEventChunk.hasNext()) {
                    ComplexEvent event = complexEventChunk.next();
                    if (event.getType() == ComplexEvent.Type.TIMER) {
                        tryFlushEvents(outputEventChunks, event, state);
                    } else if (event.getType() == ComplexEvent.Type.CURRENT) {
                        complexEventChunk.remove();
                        tryFlushEvents(outputEventChunks, event, state);
                        GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                        state.groupByKeyEvents.put(groupedComplexEvent.getGroupKey(),
                                groupedComplexEvent.getComplexEvent());
                    }
                }
            }
        } finally {
            stateHolder.returnState(state);
        }
        sendToCallBacks(outputEventChunks);
    }


    private void tryFlushEvents(List<ComplexEventChunk> outputEventChunks, ComplexEvent event, RateLimiterState state) {
        if (event.getTimestamp() >= state.scheduledTime) {
            ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>();
            for (ComplexEvent complexEvent : state.groupByKeyEvents.values()) {
                outputEventChunk.add(cloneComplexEvent(complexEvent));
            }
            outputEventChunks.add(outputEventChunk);
            state.scheduledTime += value;
            scheduler.notifyAt(state.scheduledTime);
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
        private Map<String, ComplexEvent> groupByKeyEvents = new LinkedHashMap<>();

        @Override
        public boolean canDestroy() {
            return groupByKeyEvents.isEmpty() && scheduledTime == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("GroupByKeyEvents", groupByKeyEvents);
            state.put("ScheduledTime", scheduledTime);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            groupByKeyEvents = (Map<String, ComplexEvent>) state.get("groupByKeyEvents");
            scheduledTime = (Long) state.get("ScheduledTime");
        }
    }
}

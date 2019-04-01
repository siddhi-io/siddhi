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

package org.wso2.siddhi.core.query.output.ratelimit.time;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.GroupedComplexEvent;
import org.wso2.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import org.wso2.siddhi.core.util.Schedulable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined time period and the emit only first
 * event. This implementation specifically represent GroupBy queries.
 */
public class FirstGroupByPerTimeOutputRateLimiter extends OutputRateLimiter implements Schedulable {
    private static final Logger log = Logger.getLogger(FirstGroupByPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;
    private Map<String, Long> groupByOutputTime = new HashMap();
    private ScheduledExecutorService scheduledExecutorService;
    private String queryName;

    public FirstGroupByPerTimeOutputRateLimiter(String id, Long value, ScheduledExecutorService
            scheduledExecutorService, String queryName) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.queryName = queryName;
        this.id = id;
        this.value = value;
    }

    @Override
    public OutputRateLimiter clone(String key) {
        FirstGroupByPerTimeOutputRateLimiter instance = new FirstGroupByPerTimeOutputRateLimiter(id + key, value,
                scheduledExecutorService, queryName);
        instance.setLatencyTracker(latencyTracker);
        return instance;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>(complexEventChunk.isBatch());
        complexEventChunk.reset();
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                complexEventChunk.remove();
                GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                Long outputTime = groupByOutputTime.get(groupedComplexEvent.getGroupKey());
                if (outputTime == null || outputTime + value <= currentTime) {
                    groupByOutputTime.put(groupedComplexEvent.getGroupKey(), currentTime);
                    outputEventChunk.add(groupedComplexEvent);
                }
            }
        }
        if (outputEventChunk.getFirst() != null) {
            sendToCallBacks(outputEventChunk);
        }
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        synchronized (this) {
            state.put("GroupByOutputTime", groupByOutputTime);
        }
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        synchronized (this) {
            groupByOutputTime = (Map<String, Long>) state.get("GroupByOutputTime");
        }
    }
}

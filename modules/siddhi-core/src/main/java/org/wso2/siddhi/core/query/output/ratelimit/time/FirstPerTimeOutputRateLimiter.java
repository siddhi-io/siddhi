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
import org.wso2.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import org.wso2.siddhi.core.util.Schedulable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined time period and the emit only first
 * event.
 */
public class FirstPerTimeOutputRateLimiter extends OutputRateLimiter implements Schedulable {
    private static final Logger log = Logger.getLogger(FirstPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;
    private ScheduledExecutorService scheduledExecutorService;
    private String queryName;
    private Long outputTime;


    public FirstPerTimeOutputRateLimiter(String id, Long value, ScheduledExecutorService scheduledExecutorService,
                                         String queryName) {
        this.queryName = queryName;
        this.id = id;
        this.value = value;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public OutputRateLimiter clone(String key) {
        FirstPerTimeOutputRateLimiter instance = new FirstPerTimeOutputRateLimiter(id + key, value,
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
            if (outputTime == null || outputTime + value <= currentTime) {
                outputTime = currentTime;
                ComplexEvent event = complexEventChunk.next();
                complexEventChunk.remove();
                outputEventChunk.add(event);
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
        state.put("OutputTime", outputTime);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        outputTime = (Long) state.get("OutputTime");
    }

}

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

package org.wso2.siddhi.core.query.output.ratelimit.event;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.GroupedComplexEvent;
import org.wso2.siddhi.core.query.output.ratelimit.OutputRateLimiter;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link OutputRateLimiter} which will collect pre-defined number of events and the emit only the
 * first event. This implementation specifically handle queries with group by.
 */
public class FirstGroupByPerEventOutputRateLimiter extends OutputRateLimiter {
    private final Integer value;
    private String id;
    private Map<String, Integer> groupByOutputTime = new HashMap();

    public FirstGroupByPerEventOutputRateLimiter(String id, Integer value) {
        this.id = id;
        this.value = value;
    }

    @Override
    public OutputRateLimiter clone(String key) {
        FirstGroupByPerEventOutputRateLimiter instance = new FirstGroupByPerEventOutputRateLimiter(id + key, value);
        instance.setLatencyTracker(latencyTracker);
        return instance;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<>(complexEventChunk.isBatch());
        complexEventChunk.reset();
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                complexEventChunk.remove();
                GroupedComplexEvent groupedComplexEvent = ((GroupedComplexEvent) event);
                Integer count = groupByOutputTime.get(groupedComplexEvent.getGroupKey());
                if (count == null) {
                    groupByOutputTime.put(groupedComplexEvent.getGroupKey(), 1);
                    outputEventChunk.add(groupedComplexEvent);
                } else if (count.equals(value-1)) {
                    groupByOutputTime.remove(groupedComplexEvent.getGroupKey());
                } else {
                    groupByOutputTime.put(groupedComplexEvent.getGroupKey(), count + 1);
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
            state.put("GroupByOutputCount", groupByOutputTime);
        }
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        synchronized (this) {
            groupByOutputTime = (Map<String, Integer>) state.get("GroupByOutputCount");
        }
    }

}

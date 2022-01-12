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
 * event.
 */
public class FirstPerTimeOutputRateLimiter
        extends OutputRateLimiter<FirstPerTimeOutputRateLimiter.RateLimiterState> implements Schedulable {
    private static final Logger log = LogManager.getLogger(FirstPerTimeOutputRateLimiter.class);
    private final Long value;
    private String id;

    public FirstPerTimeOutputRateLimiter(String id, Long value) {
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
                if (state.outputTime == null || state.outputTime + value <= currentTime) {
                    state.outputTime = currentTime;
                    ComplexEvent event = complexEventChunk.next();
                    complexEventChunk.remove();
                    outputEventChunk.add(event);
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

        private Long outputTime;

        @Override
        public boolean canDestroy() {
            return outputTime == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("OutputTime", outputTime);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            outputTime = (Long) state.get("OutputTime");
        }
    }

}

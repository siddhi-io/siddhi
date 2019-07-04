/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.query.selector;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.populater.StateEventPopulator;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import org.apache.log4j.Logger;

/**
 * Processor implementation representing selector portion of the Siddhi query after table optimisation
 */
public class OptimisedJoinQuerySelector extends QuerySelector {

    private static final Logger log = Logger.getLogger(OptimisedJoinQuerySelector.class);
    private String id;
    private OutputRateLimiter outputRateLimiter;
    private StateEventPopulator eventPopulator;

    public OptimisedJoinQuerySelector(String id) {
        super(id);
        this.id = id;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (log.isTraceEnabled()) {
            log.trace("event is processed by selector '" + id + "', "  + complexEventChunk);
        }

        ComplexEventChunk outputComplexEventChunk = processEventChunk(complexEventChunk);

        if (outputComplexEventChunk != null) {
            outputRateLimiter.process(outputComplexEventChunk);
        }
    }

    private ComplexEventChunk processEventChunk(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                eventPopulator.populateStateEvent(event);
            }
        }
        complexEventChunk.reset();
        if (complexEventChunk.hasNext()) {
            return complexEventChunk;
        }
        return null;
    }

    public void setNextProcessor(OutputRateLimiter outputRateLimiter) {
        if (this.outputRateLimiter == null) {
            this.outputRateLimiter = outputRateLimiter;
        } else {
            throw new SiddhiAppCreationException("outputRateLimiter is already assigned");
        }
    }

    public void setEventPopulator(StateEventPopulator eventPopulator) {
        this.eventPopulator = eventPopulator;
    }
}

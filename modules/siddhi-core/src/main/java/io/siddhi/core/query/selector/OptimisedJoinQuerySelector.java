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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.populater.StateEventPopulator;
import io.siddhi.query.api.execution.query.selection.Selector;

/**
 * Processor implementation representing selector portion of the Siddhi query after table optimisation
 */
public class OptimisedJoinQuerySelector extends QuerySelector {

    private StateEventPopulator eventPopulatorForOptimisedLookup;

    public OptimisedJoinQuerySelector(String id, Selector selector, boolean currentOn, boolean expiredOn,
                                      SiddhiQueryContext siddhiQueryContext) {
        super(id, selector, currentOn, expiredOn, siddhiQueryContext);
    }

    public void processOptimisedQueryEvents(ComplexEventChunk complexEventChunk) {

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
                eventPopulatorForOptimisedLookup.populateStateEvent(event);
            }
        }
        complexEventChunk.reset();
        if (complexEventChunk.hasNext()) {
            return complexEventChunk;
        }
        return null;
    }

    public void setEventPopulatorForOptimisedLookup(StateEventPopulator eventPopulatorForOptimisedLookup) {
        this.eventPopulatorForOptimisedLookup = eventPopulatorForOptimisedLookup;
    }

}

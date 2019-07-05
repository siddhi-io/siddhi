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
package org.wso2.siddhi.core.query.selector;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.populater.StateEventPopulator;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;

/**
 * Processor implementation representing selector portion of the Siddhi query after table optimisation
 */
public class OptimisedJoinQuerySelector extends QuerySelector {

    private static final Logger log = Logger.getLogger(OptimisedJoinQuerySelector.class);
    private StateEventPopulator eventPopulatorForOptimisedLookup;

    public OptimisedJoinQuerySelector(String id, Selector selector, boolean currentOn, boolean expiredOn,
                                      SiddhiAppContext siddhiAppContext) {
        super(id, selector, currentOn, expiredOn, siddhiAppContext);
    }

    public void processOptimisedQueryEvents(ComplexEventChunk complexEventChunk) {
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

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

package org.wso2.siddhi.core.trigger;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.core.util.statistics.ThroughputTracker;
import org.wso2.siddhi.query.api.definition.TriggerDefinition;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.wso2.siddhi.core.util.statistics.StatisticsTrackerFactory.MetricsLogLevel.INFO;

/**
 * Implementation of {@link Trigger} which will trigger events based on a pre-defined period.
 */
public class PeriodicTrigger implements Trigger {
    private TriggerDefinition triggerDefinition;
    private SiddhiAppContext siddhiAppContext;
    private StreamJunction streamJunction;
    private ScheduledFuture scheduledFuture;
    private ThroughputTracker throughputTracker;

    @Override
    public void init(TriggerDefinition triggerDefinition, SiddhiAppContext siddhiAppContext, StreamJunction
            streamJunction) {
        this.triggerDefinition = triggerDefinition;
        this.siddhiAppContext = siddhiAppContext;
        this.streamJunction = streamJunction;
        if (siddhiAppContext.getStatisticsManager() != null) {
            this.throughputTracker = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    triggerDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TRIGGERS, null, INFO);
        }
    }

    @Override
    public TriggerDefinition getTriggerDefinition() {
        return triggerDefinition;
    }

    @Override
    public String getId() {
        return triggerDefinition.getId();
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {
        scheduledFuture = siddhiAppContext.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
                if (throughputTracker != null && siddhiAppContext.isStatsEnabled()) {
                    throughputTracker.eventIn();
                }
                streamJunction.sendEvent(new Event(currentTime, new Object[]{currentTime}));
            }
        }, triggerDefinition.getAtEvery(), triggerDefinition.getAtEvery(), TimeUnit.MILLISECONDS);
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }
}

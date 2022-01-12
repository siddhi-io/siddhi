/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util.statistics;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.expression.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Log number of events received by each receiver
 **/
public class ReceivedEventCounter implements Runnable {
    private static final Logger log = LogManager.getLogger(ReceivedEventCounter.class);
    private long totalEventCounter = 0;
    private long eventCounter = 0;
    private SiddhiAppContext siddhiAppContext;
    private ScheduledFuture scheduleLogger;
    private StreamDefinition streamDefinition;
    private int duration = 1;
    private long loggerExecutionInterval;

    public void init(SiddhiAppContext siddhiAppContext, StreamDefinition streamDefinition, int duration) {
        this.siddhiAppContext = siddhiAppContext;
        this.streamDefinition = streamDefinition;
        this.duration = duration;
        this.loggerExecutionInterval = Expression.Time.minute(duration).value();
    }

    public void countEvents(Object eventObject) {
        if (eventObject instanceof Event[]) {
            eventCounter = eventCounter + ((Event[]) eventObject).length;
            totalEventCounter = totalEventCounter + ((Event[]) eventObject).length;
        } else {
            eventCounter++;
            totalEventCounter++;
        }
    }

    @Override
    public void run() {
        log.info("Event received for Stream " + streamDefinition.getId() + " in siddhi App " +
                siddhiAppContext.getName() + " for last " + duration + " minute(s): " + eventCounter + ". " +
                "Total Events: " + totalEventCounter + ".");
        eventCounter = 0;
    }

    public void scheduleEventCounterLogger() {
        if (scheduleLogger != null) {
            scheduleLogger.cancel(true);
        }
        scheduleLogger = siddhiAppContext.getScheduledExecutorService().
                scheduleWithFixedDelay(this, loggerExecutionInterval, loggerExecutionInterval,
                        TimeUnit.MILLISECONDS);
    }
}

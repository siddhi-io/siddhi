/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.stream.input.source;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.AttributeConverter;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.timestamp.TimestampGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This class wraps {@link InputHandler} class in order to guarantee exactly once processing
 */
public class InputEventHandler {

    private static final Logger LOG = LogManager.getLogger(InputEventHandler.class);

    private final ThreadLocal<Object[]> trpProperties;
    private final TimestampGenerator timestampGenerator;
    private ThreadLocal<String[]> trpSyncProperties;
    private String sourceType;
    private LatencyTracker latencyTracker;
    private SiddhiAppContext siddhiAppContext;
    private InputHandler inputHandler;
    private List<AttributeMapping> transportMapping;
    private InputEventHandlerCallback inputEventHandlerCallback;
    private long eventCount;

    InputEventHandler(InputHandler inputHandler, List<AttributeMapping> transportMapping,
                      ThreadLocal<Object[]> trpProperties, ThreadLocal<String[]> trpSyncProperties, String sourceType,
                      LatencyTracker latencyTracker, SiddhiAppContext siddhiAppContext,
                      InputEventHandlerCallback inputEventHandlerCallback) {
        this.inputHandler = inputHandler;
        this.transportMapping = transportMapping;
        this.trpProperties = trpProperties;
        this.trpSyncProperties = trpSyncProperties;
        this.sourceType = sourceType;
        this.latencyTracker = latencyTracker;
        this.siddhiAppContext = siddhiAppContext;
        this.inputEventHandlerCallback = inputEventHandlerCallback;
        this.timestampGenerator = siddhiAppContext.getTimestampGenerator();
    }

    public void sendEvent(Event event) throws InterruptedException {
        try {
            if (latencyTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTracker.markOut();
            }
            Object[] transportProperties = trpProperties.get();
            String[] transportSyncProperties = trpSyncProperties.get();
            if (event.getTimestamp() == -1) {
                long currentTimestamp = timestampGenerator.currentTime();
                event.setTimestamp(currentTimestamp);
            }
            for (int i = 0; i < transportMapping.size(); i++) {
                AttributeMapping attributeMapping = transportMapping.get(i);
                event.getData()[attributeMapping.getPosition()] = AttributeConverter.getPropertyValue(
                        transportProperties[i], attributeMapping.getType());
            }
            inputEventHandlerCallback.sendEvent(event, transportSyncProperties);
            eventCount++;
        } catch (RuntimeException e) {
            LOG.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                    " Error in applying transport property mapping for '" + sourceType
                    + "' source at '" + inputHandler.getStreamId() + "' stream.", e);
        }
    }

    public void sendEvents(Event[] events) throws InterruptedException {
        try {
            if (latencyTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTracker.markOut();
            }
            Object[] transportProperties = trpProperties.get();
            trpProperties.remove();
            String[] transportSyncProperties = trpSyncProperties.get();
            trpSyncProperties.remove();
            long currentTimestamp = timestampGenerator.currentTime();
            for (Event event : events) {
                if (event.getTimestamp() == -1) {
                    event.setTimestamp(currentTimestamp);
                }
                for (int i = 0; i < transportMapping.size(); i++) {
                    AttributeMapping attributeMapping = transportMapping.get(i);
                    event.getData()[attributeMapping.getPosition()] = AttributeConverter.getPropertyValue(
                            transportProperties[i], attributeMapping.getType());
                }
            }
            inputEventHandlerCallback.sendEvents(events, transportSyncProperties);
            eventCount += events.length;
        } catch (RuntimeException e) {
            LOG.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                    " Error in applying transport property mapping for '" + sourceType
                    + "' source at '" + inputHandler.getStreamId() + "' stream.", e);
        } finally {
            trpProperties.remove();
            trpSyncProperties.remove();
        }
    }

    long getEventCount() {
        return eventCount;
    }
}

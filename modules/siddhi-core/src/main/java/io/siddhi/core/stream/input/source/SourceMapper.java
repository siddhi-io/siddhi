/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.exception.MappingFailedException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.util.ErrorOccurrence;
import io.siddhi.core.util.error.handler.util.ErrorStoreHelper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.ReceivedEventCounter;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

import static io.siddhi.core.util.SiddhiConstants.ENABLE_EVENT_COUNT_LOGGER;
import static io.siddhi.core.util.SiddhiConstants.LOGGING_DURATION;
import static io.siddhi.core.util.SiddhiConstants.TRUE;

/**
 * Convert custom input from {@link Source} to {@link ComplexEventChunk}.
 */
public abstract class SourceMapper implements SourceEventListener {

    private static final Logger log = LogManager.getLogger(SourceMapper.class);
    private final ThreadLocal<Object[]> trpProperties = new ThreadLocal<>();
    private final ThreadLocal<String[]> trpSyncProperties = new ThreadLocal<>();
    protected String sourceType;
    protected OptionHolder sourceOptionHolder;
    private InputEventHandler inputEventHandler;
    private StreamDefinition streamDefinition;
    private String mapType;
    private List<AttributeMapping> transportMappings;
    private SourceHandler sourceHandler;
    private SiddhiAppContext siddhiAppContext;
    private ThroughputTracker throughputTracker;
    private LatencyTracker mapperLatencyTracker;
    private int loggingDuration = 1;
    private boolean logEventCount = false;
    private ReceivedEventCounter receivedEventCounter;
    private boolean firstRun = true;

    public final void init(StreamDefinition streamDefinition, String mapType, OptionHolder mapOptionHolder,
                           List<AttributeMapping> attributeMappings, String sourceType,
                           SourceSyncCallback sourceSyncCallback, List<AttributeMapping> transportMappings,
                           SourceHandler sourceHandler, OptionHolder sourceOptionHolder, ConfigReader configReader,
                           SiddhiAppContext siddhiAppContext) {

        this.streamDefinition = streamDefinition;
        this.mapType = mapType;
        this.sourceType = sourceType;
        this.transportMappings = transportMappings;
        this.sourceOptionHolder = sourceOptionHolder;
        if (sourceHandler != null) {
            sourceHandler.initSourceHandler(siddhiAppContext.getName(), sourceSyncCallback, streamDefinition,
                    siddhiAppContext);
        }
        this.sourceHandler = sourceHandler;
        this.siddhiAppContext = siddhiAppContext;
        if (siddhiAppContext.getStatisticsManager() != null) {
            this.throughputTracker = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    streamDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_SOURCES, sourceType);
            this.mapperLatencyTracker = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                    streamDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_SOURCE_MAPPERS,
                    sourceType + SiddhiConstants.METRIC_DELIMITER + mapType);
        }
        if (configReader != null && configReader.getAllConfigs().size() != 0) {
            if (configReader.getAllConfigs().containsKey(ENABLE_EVENT_COUNT_LOGGER) &&
                    configReader.getAllConfigs().get(ENABLE_EVENT_COUNT_LOGGER).toLowerCase().equals(TRUE)) {
                logEventCount = true;
                this.receivedEventCounter = new ReceivedEventCounter();
                if (configReader.getAllConfigs().containsKey(LOGGING_DURATION)) {
                    loggingDuration = Integer.parseInt(configReader.getAllConfigs().get(LOGGING_DURATION));
                }
                receivedEventCounter.init(siddhiAppContext, streamDefinition, loggingDuration);
            }
        }
        init(streamDefinition, mapOptionHolder, attributeMappings, configReader, siddhiAppContext);
    }

    /**
     * Initialize Source-mapper
     *
     * @param streamDefinition     Associated output stream definition
     * @param optionHolder         Mapper option holder
     * @param attributeMappingList Custom attribute mapping for source-mapping
     * @param configReader         System configuration reader
     * @param siddhiAppContext     Siddhi application context
     */
    public abstract void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping>
            attributeMappingList, ConfigReader configReader, SiddhiAppContext siddhiAppContext);

    /**
     * Support classes that the source-mapper can consume for mapping processing (used for validation purposes)
     *
     * @return Supported event classes that mapper can process.
     */
    public abstract Class[] getSupportedInputEventClasses();

    public final void setInputHandler(InputHandler inputHandler) {

        InputEventHandlerCallback inputEventHandlerCallback;
        if (sourceHandler != null) {
            sourceHandler.setInputHandler(inputHandler);
            inputEventHandlerCallback = sourceHandler;
        } else {
            inputEventHandlerCallback = new PassThroughSourceHandler(inputHandler);
        }
        this.inputEventHandler = new InputEventHandler(inputHandler, transportMappings,
                trpProperties, trpSyncProperties, sourceType, mapperLatencyTracker, siddhiAppContext,
                inputEventHandlerCallback);
    }

    public final void onEvent(Object eventObject, String[] transportProperties) {
        Object[] transportPropertyObjects = convertToObjectArray(transportProperties);
        onEvent(eventObject, transportPropertyObjects, null);
    }

    public final void onEvent(Object eventObject, Object[] transportProperties) {
        onEvent(eventObject, transportProperties, null);
    }

    public final void onEvent(Object eventObject, String[] transportProperties, String[] transportSyncProperties) {
        Object[] transportPropertyObjects = convertToObjectArray(transportProperties);
        onEvent(eventObject, transportPropertyObjects, transportSyncProperties);

    }

    private Object[] convertToObjectArray(String[] transportProperties) {
        Object[] transportPropertyObjects = null;
        if (transportProperties != null) {
            transportPropertyObjects = new Object[transportProperties.length];
            for (int i = 0, transportPropertiesLength = transportProperties.length;
                 i < transportPropertiesLength; i++) {
                String transportProperty = transportProperties[i];
                transportPropertyObjects[i] = transportProperty;
            }
        }
        return transportPropertyObjects;
    }

    public final void onEvent(Object eventObject, Object[] transportProperties, String[] transportSyncProperties) {
        try {
            if (eventObject != null) {
                if (!allowNullInTransportProperties() && transportProperties != null) {
                    for (Object property : transportProperties) {
                        if (property == null) {
                            log.error("Dropping event " + eventObject.toString() + " belonging to stream " +
                                    streamDefinition.getId()
                                    + " as it contains null transport properties and system "
                                    + "is configured to not allow null transport properties. You can "
                                    + "configure it via source mapper if the respective "
                                    + "mapper type allows it. Refer mapper documentation to verify "
                                    + "supportability");
                            return;
                        }
                    }
                }
                trpProperties.set(transportProperties);
                if (transportSyncProperties != null) {
                    trpSyncProperties.set(transportSyncProperties);
                }
                try {
                    if (throughputTracker != null &&
                            Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                        throughputTracker.eventIn();
                    }
                    if (throughputTracker != null &&
                            Level.DETAIL.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                        mapperLatencyTracker.markIn();
                    }
                    mapAndProcess(eventObject, inputEventHandler);
                    if (logEventCount) {
                        if (firstRun) {
                            receivedEventCounter.scheduleEventCounterLogger();
                            firstRun = false;
                        }
                        receivedEventCounter.countEvents(eventObject);
                    }
                } finally {
                    if (throughputTracker != null &&
                            Level.DETAIL.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                        mapperLatencyTracker.markOut();
                    }
                }
            }
        } catch (MappingFailedException e) {
            ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                    ErrorOccurrence.BEFORE_SOURCE_MAPPING, siddhiAppContext.getName(), e.getFailures(),
                    streamDefinition.getId());
            log.error("Error while processing '" + eventObject + "', for the input Mapping '" + mapType +
                    "' for the stream '" + streamDefinition.getId() + "'.", e);
        } catch (InterruptedException | RuntimeException e) {
            ErroneousEvent erroneousEvent = new ErroneousEvent(eventObject, e, "Error while processing '" +
                    eventObject + "', for the input Mapping '" + mapType + "' for the stream '" +
                    streamDefinition.getId() + "'.");
            ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                    ErrorOccurrence.BEFORE_SOURCE_MAPPING, siddhiAppContext.getName(),
                    Collections.singletonList(erroneousEvent), streamDefinition.getId());
            log.error("Error while processing '" + eventObject + "', for the input Mapping '" + mapType +
                    "' for the stream '" + streamDefinition.getId() + "'.", e);
        } finally {
            trpProperties.remove();
            if (transportSyncProperties != null) {
                trpSyncProperties.remove();
            }
        }
    }

    public SourceHandler getHandler() {

        return this.sourceHandler;
    }

    public final StreamDefinition getStreamDefinition() {

        return streamDefinition;
    }

    public long getEventCount() {
        return this.inputEventHandler.getEventCount();
    }

    /**
     * Method to map the incoming event and as pass that via inputEventHandler to process further.
     *
     * @param eventObject       Incoming event Object
     * @param inputEventHandler Handler to pass the converted Siddhi Event for processing
     * @throws MappingFailedException Throws MappingFailedException
     * @throws InterruptedException   Throws InterruptedException
     */
    protected abstract void mapAndProcess(Object eventObject,
                                          InputEventHandler inputEventHandler)
            throws MappingFailedException, InterruptedException;

    /**
     * Method used by {@link SourceMapper} to determine on how to handle transport properties with null values. If
     * this returns 'false' then {@link SourceMapper} will drop any event/s with null transport
     * property values. If this returns
     * 'true' then {@link SourceMapper} will send events even though they contains null transport properties.
     * This method will be called after init().
     *
     * @return whether {@link SourceMapper} should allow or drop events when transport properties are null.
     */
    protected abstract boolean allowNullInTransportProperties();
}

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

package org.wso2.siddhi.core.stream.output.sink;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.distributed.DistributedTransport;
import org.wso2.siddhi.core.util.ExceptionUtil;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.StringUtil;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;
import org.wso2.siddhi.core.util.statistics.ThroughputTracker;
import org.wso2.siddhi.core.util.transport.BackoffRetryCounter;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a Sink type. these let users to publish events according to
 * some type. this type can either be local, jms or ws (or any custom extension)
 */
public abstract class Sink implements SinkListener, Snapshotable {

    private static final Logger LOG = Logger.getLogger(Sink.class);
    private StreamDefinition streamDefinition;
    private String type;
    private SinkMapper mapper;
    private SinkHandler handler;
    private DistributedTransport.ConnectionCallback connectionCallback = null;
    private String elementId;
    private SiddhiAppContext siddhiAppContext;
    private OnErrorAction onErrorAction;

    protected AtomicBoolean isTryingToConnect = new AtomicBoolean(false);
    private BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
    private BackoffRetryCounter backoffPublishRetryCounter = new BackoffRetryCounter();
    private AtomicBoolean isConnected = new AtomicBoolean(false);
    private ThreadLocal<DynamicOptions> trpDynamicOptions;
    private ScheduledExecutorService scheduledExecutorService;
    private ThroughputTracker throughputTracker;
    private LatencyTracker mapperLatencyTracker;

    public final void init(StreamDefinition streamDefinition, String type, OptionHolder transportOptionHolder,
                           ConfigReader sinkConfigReader, SinkMapper sinkMapper, String mapType,
                           OptionHolder mapOptionHolder, SinkHandler sinkHandler, List<Element> payloadElementList,
                           ConfigReader mapperConfigReader, SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.type = type;
        this.elementId = siddhiAppContext.getElementIdGenerator().createNewId();
        this.siddhiAppContext = siddhiAppContext;
        this.onErrorAction = OnErrorAction.valueOf(transportOptionHolder
                .getOrCreateOption(SiddhiConstants.ANNOTATION_ELEMENT_ON_ERROR, "LOG")
                .getValue().toUpperCase());
        if (siddhiAppContext.getStatisticsManager() != null) {
            this.throughputTracker = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    streamDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_SINKS, type);
            this.mapperLatencyTracker = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                    streamDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_SINK_MAPPERS,
                    type + SiddhiConstants.METRIC_DELIMITER + mapType);
        }
        init(streamDefinition, transportOptionHolder, sinkConfigReader, siddhiAppContext);
        if (sinkMapper != null) {
            sinkMapper.init(streamDefinition, mapType, mapOptionHolder, payloadElementList, this,
                    mapperConfigReader, mapperLatencyTracker, siddhiAppContext);
            this.mapper = sinkMapper;
        }
        if (sinkHandler != null) {
            sinkHandler.initSinkHandler(siddhiAppContext.getElementIdGenerator().createNewId(), streamDefinition,
                    new SinkHandlerCallback(sinkMapper));
            this.handler = sinkHandler;
        }

        scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();

    }

    public abstract Class[] getSupportedInputEventClasses();


    public final void initOnlyTransport(StreamDefinition streamDefinition, OptionHolder transportOptionHolder,
                                        ConfigReader sinkConfigReader, String type,
                                        DistributedTransport.ConnectionCallback connectionCallback,
                                        SiddhiAppContext siddhiAppContext) {
        this.type = type;
        this.streamDefinition = streamDefinition;
        this.connectionCallback = connectionCallback;
        this.elementId = siddhiAppContext.getElementIdGenerator().createNewId();
        this.siddhiAppContext = siddhiAppContext;
        init(streamDefinition, transportOptionHolder, sinkConfigReader, siddhiAppContext);
        scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
    }

    /**
     * Supported dynamic options by the transport
     *
     * @return the list of supported dynamic option keys
     */
    public abstract String[] getSupportedDynamicOptions();

    /**
     * Will be called for initialing the {@link Sink}
     *
     * @param outputStreamDefinition containing stream definition bind to the {@link Sink}
     * @param optionHolder           Option holder containing static and dynamic options related to the {@link Sink}
     * @param sinkConfigReader       this hold the {@link Sink} extensions configuration reader.
     * @param siddhiAppContext       {@link SiddhiAppContext} of the parent siddhi app.
     */
    protected abstract void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                 ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext);

    @Override
    public final void publish(Object payload) {
        if (mapperLatencyTracker != null && siddhiAppContext.isStatsEnabled()) {
            mapperLatencyTracker.markOut();
        }
        if (isConnected()) {
            try {
                DynamicOptions dynamicOptions = trpDynamicOptions.get();
                publish(payload, dynamicOptions);
                if (throughputTracker != null && siddhiAppContext.isStatsEnabled()) {
                    throughputTracker.eventIn();
                }
            } catch (ConnectionUnavailableException e) {
                setConnected(false);
                if (connectionCallback != null) {
                    connectionCallback.connectionFailed();
                }
                LOG.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Connection unavailable at Sink '" + type + "' at '" + streamDefinition.getId() +
                        "', will retry connection immediately.", e);
                connectWithRetry();
                publish(payload);
            }
        } else if (isTryingToConnect.get()) {
            onError(payload);
        } else {
            connectWithRetry();
            publish(payload);
        }
    }

    /**
     * Sending events via output transport
     *
     * @param payload          payload of the event
     * @param transportOptions one of the event constructing the payload
     * @throws ConnectionUnavailableException throw when connections are unavailable.
     */
    public abstract void publish(Object payload, DynamicOptions transportOptions)
            throws ConnectionUnavailableException;


    /**
     * Called to connect to the backend before events are published
     *
     * @throws ConnectionUnavailableException if it cannot connect to the backend
     */
    public abstract void connect() throws ConnectionUnavailableException;

    /**
     * Called after all publishing is done, or when ConnectionUnavailableException is thrown
     */
    public abstract void disconnect();

    /**
     * Called at the end to clean all the resources consumed
     */
    public abstract void destroy();

    public final String getType() {
        return type;
    }

    public final SinkMapper getMapper() {
        return mapper;
    }

    public final SinkHandler getHandler() {
        return handler;
    }

    public void connectWithRetry() {
        if (!isConnected.get()) {
            isTryingToConnect.set(true);
            try {
                connect();
                setConnected(true);
                isTryingToConnect.set(false);
                if (connectionCallback != null) {
                    connectionCallback.connectionEstablished();
                }
                backoffRetryCounter.reset();
            } catch (ConnectionUnavailableException e) {
                LOG.error(StringUtil.removeCRLFCharacters(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Error while connecting at Sink '" + type + "' at '" + streamDefinition.getId() +
                        "', will retry in '" + backoffRetryCounter.getTimeInterval() + "'."), e);
                scheduledExecutorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        connectWithRetry();
                    }
                }, backoffRetryCounter.getTimeIntervalMillis(), TimeUnit.MILLISECONDS);
                backoffRetryCounter.increment();
            } catch (RuntimeException e) {
                LOG.error(StringUtil.removeCRLFCharacters(ExceptionUtil.getMessageWithContext(e, siddhiAppContext)) +
                        " Error while connecting at Sink '" + StringUtil.removeCRLFCharacters(type) + "' at '" +
                        StringUtil.removeCRLFCharacters(streamDefinition.getId()) + "'.", e);
                throw e;
            }
        }
    }

    public void shutdown() {
        disconnect();
        destroy();
        setConnected(false);
        isTryingToConnect.set(false);
        if (connectionCallback != null) {
            connectionCallback.connectionFailed();
        }
    }

    @Override
    public final String getElementId() {
        return elementId;
    }

    void setTrpDynamicOptions(ThreadLocal<DynamicOptions> trpDynamicOptions) {
        this.trpDynamicOptions = trpDynamicOptions;
    }

    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public boolean isConnected() {
        return isConnected.get();
    }

    public void setConnected(boolean connected) {
        isConnected.set(connected);
    }

    void onError(Object payload) {
        switch (onErrorAction) {
            case LOG:
                LOG.error("Error on '" + siddhiAppContext.getName() + "'. Dropping event at Sink '"
                        + type + "' at '" + streamDefinition.getId() + "' as its still trying to reconnect!, "
                        + "events dropped '" + payload + "'");
                break;
            case WAIT:
                retryWait(backoffPublishRetryCounter.getTimeIntervalMillis());
                backoffPublishRetryCounter.increment();
                publish(payload);
                break;
            default:
                break;
        }
    }

    private enum OnErrorAction {
        LOG,
        WAIT
    }

    private void retryWait(long waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException ignored) {
        }
    }
}

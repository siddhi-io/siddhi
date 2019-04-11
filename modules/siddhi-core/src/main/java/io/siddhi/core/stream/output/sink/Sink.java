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

package io.siddhi.core.stream.output.sink;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.distributed.DistributedTransport;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.StringUtil;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.transport.BackoffRetryCounter;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a Sink type. these let users to publish events according to
 * some type. this type can either be local, jms or ws (or any custom extension)
 *
 * @param <S> current state of the Sink
 */
public abstract class Sink<S extends State> implements SinkListener {

    private static final Logger LOG = Logger.getLogger(Sink.class);
    protected AtomicBoolean isTryingToConnect = new AtomicBoolean(false);
    private StreamDefinition streamDefinition;
    private String type;
    private SinkMapper mapper;
    private SinkHandler handler;
    private DistributedTransport.ConnectionCallback connectionCallback = null;
    private SiddhiAppContext siddhiAppContext;
    private OnErrorAction onErrorAction;
    private BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
    private BackoffRetryCounter backoffPublishRetryCounter = new BackoffRetryCounter();
    private AtomicBoolean isConnected = new AtomicBoolean(false);
    private ThreadLocal<DynamicOptions> trpDynamicOptions;
    private ScheduledExecutorService scheduledExecutorService;
    private ThroughputTracker throughputTracker;
    private LatencyTracker mapperLatencyTracker;
    private StateHolder<S> stateHolder;
    private ServiceDeploymentInfo serviceDeploymentInfo;

    public final void init(StreamDefinition streamDefinition, String type, OptionHolder transportOptionHolder,
                           ConfigReader sinkConfigReader, SinkMapper sinkMapper, String mapType,
                           OptionHolder mapOptionHolder, SinkHandler sinkHandler, List<Element> payloadElementList,
                           ConfigReader mapperConfigReader, Map<String, String> deploymentProperties,
                           SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.type = type;
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
        StateFactory<S> stateFactory = init(streamDefinition, transportOptionHolder, sinkConfigReader,
                siddhiAppContext);
        stateHolder = siddhiAppContext.generateStateHolder(streamDefinition.getId() + "-" +
                this.getClass().getName(), stateFactory);
        if (sinkMapper != null) {
            sinkMapper.init(streamDefinition, mapType, mapOptionHolder, payloadElementList, this,
                    mapperConfigReader, mapperLatencyTracker, siddhiAppContext);
            this.mapper = sinkMapper;
        }
        if (sinkHandler != null) {
            sinkHandler.initSinkHandler(siddhiAppContext.getName(), streamDefinition,
                    new SinkHandlerCallback(sinkMapper), siddhiAppContext);
            this.handler = sinkHandler;
        }
        scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        serviceDeploymentInfo = exposeServiceDeploymentInfo();
        if (serviceDeploymentInfo != null) {
            serviceDeploymentInfo.addDeploymentProperties(deploymentProperties);
        } else if (!deploymentProperties.isEmpty()) {
            throw new SiddhiAppCreationException("Deployment properties '" + deploymentProperties +
                    "' are defined for sink '" + type + "' which does not expose a service");
        }
    }

    public abstract Class[] getSupportedInputEventClasses();


    public final void initOnlyTransport(StreamDefinition streamDefinition, OptionHolder transportOptionHolder,
                                        ConfigReader sinkConfigReader, String type,
                                        DistributedTransport.ConnectionCallback connectionCallback,
                                        Map<String, String> deploymentProperties, SiddhiAppContext siddhiAppContext) {
        this.type = type;
        this.streamDefinition = streamDefinition;
        this.connectionCallback = connectionCallback;
        this.siddhiAppContext = siddhiAppContext;
        init(streamDefinition, transportOptionHolder, sinkConfigReader, siddhiAppContext);
        scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        serviceDeploymentInfo = exposeServiceDeploymentInfo();
        if (serviceDeploymentInfo != null) {
            serviceDeploymentInfo.addDeploymentProperties(deploymentProperties);
        } else if (!deploymentProperties.isEmpty()) {
            throw new SiddhiAppCreationException("Deployment properties '" + deploymentProperties +
                    "' are defined for sink '" + type + "' which does not expose a service");
        }
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    protected abstract ServiceDeploymentInfo exposeServiceDeploymentInfo();

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
    protected abstract StateFactory<S> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                            ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext);

    @Override
    public final void publish(Object payload) {
        if (mapperLatencyTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            mapperLatencyTracker.markOut();
        }
        if (isConnected()) {
            S state = stateHolder.getState();
            try {
                DynamicOptions dynamicOptions = trpDynamicOptions.get();
                publish(payload, dynamicOptions, state);
                if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
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
            } finally {
                stateHolder.returnState(state);
            }
        } else if (isTryingToConnect.get()) {
            onError(payload, new SiddhiAppRuntimeException("Connection unavailable at Sink '" + type + "' at '"
                    + streamDefinition.getId() + "'. Connection retrying is in progress from a different thread."));
        } else {
            connectWithRetry();
            publish(payload);
        }
    }

    /**
     * Sending events via output transport
     *
     * @param payload        payload of the event
     * @param dynamicOptions of the event constructing the payload
     * @param state          current state of the sink
     * @throws ConnectionUnavailableException throw when connections are unavailable.
     */
    public abstract void publish(Object payload, DynamicOptions dynamicOptions, S state)
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

    void onError(Object payload, Exception e) {
        switch (onErrorAction) {
            case STREAM:
                throw new SiddhiAppRuntimeException("Dropping event at Sink '"
                        + type + "' at '" + streamDefinition.getId() + "' as its still trying to reconnect!, "
                        + "event dropped '" + payload + "'", e);
            case WAIT:
                retryWait(backoffPublishRetryCounter.getTimeIntervalMillis());
                backoffPublishRetryCounter.increment();
                publish(payload);
                break;
            case LOG:
            default:
                LOG.error("Error on '" + siddhiAppContext.getName() + "'. Dropping event at Sink '"
                        + type + "' at '" + streamDefinition.getId() + "' as its still trying to reconnect!, "
                        + "events dropped '" + payload + "'");
                break;
        }
    }

    public List<ServiceDeploymentInfo> getServiceDeploymentInfoList() {
        if (serviceDeploymentInfo != null) {
            List<ServiceDeploymentInfo> list = new ArrayList<>(1);
            list.add(serviceDeploymentInfo);
            return list;
        } else {
            return new ArrayList<>(0);
        }
    }

    private void retryWait(long waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Different Type of On Error Actions
     */
    public enum OnErrorAction {
        LOG,
        WAIT,
        STREAM
    }
}

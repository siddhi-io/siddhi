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

package io.siddhi.core.stream.output.sink.distributed;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.stream.output.sink.SinkHandler;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the base class for Distributed transports. All distributed transport types must inherit from this class
 */
public abstract class DistributedTransport extends Sink {
    private static final Logger log = LogManager.getLogger(DistributedTransport.class);
    protected DistributionStrategy strategy;
    protected StreamDefinition streamDefinition;
    protected SiddhiAppContext siddhiAppContext;
    private String type;
    private OptionHolder sinkOptionHolder;
    private String[] supportedDynamicOptions;

    /**
     * Will be called for initialing the {@link Sink}
     *
     * @param outputStreamDefinition The stream definition this Output transport/sink is attached to
     * @param optionHolder           Option holder containing static and dynamic options related to the
     *                               {@link Sink}
     * @param sinkConfigReader       this hold the {@link Sink} extensions configuration reader.
     * @param siddhiAppContext       Context of the siddhi app which this output sink belongs to
     */
    @Override
    protected StateFactory<State> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = outputStreamDefinition;
        this.sinkOptionHolder = optionHolder;
        this.siddhiAppContext = siddhiAppContext;
        return null;
    }

    /**
     * This is method contains the additional parameters which require to initialize distributed transport
     *
     * @param streamDefinition                Definition of the stream this sink instance is publishing to
     * @param type                            Type of the transport that (e.g., TCP, JMS)
     * @param transportOptionHolder           Option holder for carrying options for the transport
     * @param sinkConfigReader                This hold the {@link Sink} extensions configuration reader for the sink
     * @param sinkMapper                      Hold the mapper that's used in this sink
     * @param mapType                         Type of the mapper
     * @param mapOptionHolder                 Options of the mapper
     * @param sinkHandler                     Sink handler to do optional processing
     * @param payloadElementList              The template list of the payload messages
     * @param mapperConfigReader              This hold the {@link Sink} extensions configuration reader for the mapper
     * @param streamJunction
     * @param siddhiAppContext                The siddhi app context
     * @param destinationOptionHolders        List of option holders containing the options mentioned in @destination
     * @param sinkAnnotation                  The annotation of the Sink
     * @param strategy                        Publishing strategy to be used by the distributed transport
     * @param supportedDynamicOptions         List of supported dynamic options
     * @param deploymentProperties            Service deployment properties for distributed transport sink
     * @param destinationDeploymentProperties Service deployment properties
     */
    public void init(StreamDefinition streamDefinition, String type, OptionHolder transportOptionHolder,
                     ConfigReader sinkConfigReader,
                     SinkMapper sinkMapper, String mapType, OptionHolder mapOptionHolder, SinkHandler sinkHandler,
                     List<Element> payloadElementList, ConfigReader mapperConfigReader,
                     StreamJunction streamJunction, SiddhiAppContext siddhiAppContext,
                     List<OptionHolder> destinationOptionHolders, Annotation sinkAnnotation,
                     DistributionStrategy strategy, String[] supportedDynamicOptions,
                     Map<String, String> deploymentProperties,
                     List<Map<String, String>> destinationDeploymentProperties) {
        this.type = type;
        this.strategy = strategy;
        this.supportedDynamicOptions = supportedDynamicOptions;
        init(streamDefinition, type, transportOptionHolder, sinkConfigReader, sinkMapper, mapType, mapOptionHolder,
                sinkHandler, payloadElementList, mapperConfigReader, new HashMap<>(),
                streamJunction, siddhiAppContext);
        initTransport(sinkOptionHolder, destinationOptionHolders, deploymentProperties,
                destinationDeploymentProperties, sinkAnnotation, sinkConfigReader, strategy, type, siddhiAppContext);
    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions, State state)
            throws ConnectionUnavailableException {
        int errorCount = 0;
        StringBuilder errorMessages = null;
        List<Integer> destinationsToPublish = strategy.getDestinationsToPublish(payload, transportOptions);
        int destinationsToPublishSize = destinationsToPublish.size();
        if (destinationsToPublishSize == 0) {
            throw new ConnectionUnavailableException("Error on '" + siddhiAppContext.getName() + "' at Sink '" + type +
                    "' stream '" + streamDefinition.getId() + "' as no connections are available to publish data.");
        }
        for (Integer destinationId : destinationsToPublish) {
            try {
                publish(payload, transportOptions, destinationId, state);
            } catch (ConnectionUnavailableException e) {
                errorCount++;
                if (errorMessages == null) {
                    errorMessages = new StringBuilder();
                }
                errorMessages.append("[Destination ").append(destinationId).append("]:").append(e.getMessage());
            }
        }

        if (errorCount == destinationsToPublish.size()) {
            throw new ConnectionUnavailableException("Error on '" + siddhiAppContext.getName() + "'. " + errorCount +
                    "/" + destinationsToPublish.size() + " connections failed while trying to publish with following" +
                    " error messages:" + errorMessages.toString());
        }
    }

    @Override
    public boolean isConnected() {
        return strategy.getActiveDestinationCount() > 0;
    }

    /**
     * Supported dynamic options by the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return supportedDynamicOptions;
    }

    public abstract void publish(Object payload, DynamicOptions transportOptions, Integer destinationId, State state)
            throws ConnectionUnavailableException;


    public abstract void initTransport(OptionHolder sinkOptionHolder, List<OptionHolder> destinationOptionHolders,
                                       Map<String, String> deploymentProperties,
                                       List<Map<String, String>> destinationDeploymentProperties,
                                       Annotation sinkAnnotation, ConfigReader sinkConfigReader,
                                       DistributionStrategy strategy, String type, SiddhiAppContext siddhiAppContext);

    /**
     * Connection callback to notify DistributionStrategy about new connection initiations and failures
     */
    public abstract class ConnectionCallback {

        public abstract void connectionEstablished();

        public abstract void connectionFailed();
    }
}


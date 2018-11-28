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

package org.wso2.siddhi.core.stream.output.sink.distributed;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.stream.output.sink.SinkHandler;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.ExceptionUtil;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

/**
 * This is the base class for Distributed transports. All distributed transport types must inherit from this class
 */
public abstract class DistributedTransport extends Sink {
    private static final Logger log = Logger.getLogger(DistributedTransport.class);
    private String type;
    protected DistributionStrategy strategy;
    protected StreamDefinition streamDefinition;
    protected SiddhiAppContext siddhiAppContext;
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
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext
                                siddhiAppContext) {
        this.streamDefinition = outputStreamDefinition;
        this.sinkOptionHolder = optionHolder;
        this.siddhiAppContext = siddhiAppContext;
    }

    /**
     * This is method contains the additional parameters which require to initialize distributed transport
     *
     * @param streamDefinition         Definition of the stream this sink instance is publishing to
     * @param type                     Type of the transport that (e.g., TCP, JMS)
     * @param transportOptionHolder    Option holder for carrying options for the transport
     * @param sinkConfigReader         This hold the {@link Sink} extensions configuration reader for the sink
     * @param sinkMapper               Hold the mapper that's used in this sink
     * @param mapType                  Type of the mapper
     * @param mapOptionHolder          Options of the mapper
     * @param sinkHandler              Sink handler to do optional processing
     * @param payloadElementList       The template list of the payload messages
     * @param mapperConfigReader       This hold the {@link Sink} extensions configuration reader for the mapper
     * @param siddhiAppContext         The siddhi app context
     * @param destinationOptionHolders List of option holders containing the options mentioned in @destination
     * @param sinkAnnotation           The annotation of the Sink
     * @param strategy                 Publishing strategy to be used by the distributed transport
     * @param supportedDynamicOptions  List of supported dynamic options
     */
    public void init(StreamDefinition streamDefinition, String type, OptionHolder transportOptionHolder,
                     ConfigReader sinkConfigReader,
                     SinkMapper sinkMapper, String mapType, OptionHolder mapOptionHolder, SinkHandler sinkHandler,
                     List<Element> payloadElementList, ConfigReader mapperConfigReader,
                     SiddhiAppContext siddhiAppContext, List<OptionHolder> destinationOptionHolders,
                     Annotation sinkAnnotation, DistributionStrategy strategy, String[] supportedDynamicOptions) {
        this.type = type;
        this.strategy = strategy;
        this.supportedDynamicOptions = supportedDynamicOptions;
        init(streamDefinition, type, transportOptionHolder, sinkConfigReader, sinkMapper, mapType, mapOptionHolder,
                sinkHandler, payloadElementList, mapperConfigReader, siddhiAppContext);
        initTransport(sinkOptionHolder, destinationOptionHolders, sinkAnnotation, sinkConfigReader, strategy, type,
                siddhiAppContext);
    }

    @Override
    public void publish(Object payload, DynamicOptions transportOptions) throws ConnectionUnavailableException {
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
                publish(payload, transportOptions, destinationId);
            } catch (ConnectionUnavailableException e) {
                errorCount++;
                if (errorMessages == null) {
                    errorMessages = new StringBuilder();
                }
                errorMessages.append("[Destination ").append(destinationId).append("]:").append(e.getMessage());
                log.warn(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) + " Failed to publish destination ID "
                        + destinationId);
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

    public abstract void publish(Object payload, DynamicOptions transportOptions, Integer destinationId)
            throws ConnectionUnavailableException;


    public abstract void initTransport(OptionHolder sinkOptionHolder, List<OptionHolder> destinationOptionHolders,
                                       Annotation sinkAnnotation, ConfigReader sinkConfigReader,
                                       DistributionStrategy strategy, String type, SiddhiAppContext siddhiAppContext);

    public void connectWithRetry() {
        if (!isConnected()) {
            isTryingToConnect.set(true);
            try {
                connect();
            } catch (ConnectionUnavailableException ignored) {

            }
            int retryAttempt = 0;
            while (strategy.getActiveDestinationCount() == 0 && retryAttempt < 4) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {

                }
                retryAttempt++;
            }
        }
    }

    /**
     * Connection callback to notify DistributionStrategy about new connection initiations and failures
     */
    public abstract class ConnectionCallback {

        public abstract void connectionEstablished();

        public abstract void connectionFailed();
    }
}


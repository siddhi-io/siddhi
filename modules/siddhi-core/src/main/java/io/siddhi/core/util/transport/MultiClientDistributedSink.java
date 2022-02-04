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

package io.siddhi.core.util.transport;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.stream.output.sink.distributed.DistributedTransport;
import io.siddhi.core.stream.output.sink.distributed.DistributionStrategy;
import io.siddhi.core.util.SiddhiClassLoader;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.holder.SinkExecutorExtensionHolder;
import io.siddhi.core.util.parser.helper.DefinitionParserHelper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.extension.Extension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is the distributed transport to publish to multiple endpoints using client/publisher for each endpoint. There
 * will be a separate independent {@link Sink} instance connecting to each destination. This class interacts
 * with Sink interface and it does not make any assumptions on the underlying transport implementation.
 */
public class MultiClientDistributedSink extends DistributedTransport {
    private static final Logger log = LogManager.getLogger(MultiClientDistributedSink.class);

    private List<Sink> transports = new ArrayList<>();

    @Override
    public void publish(Object payload, DynamicOptions transportOptions, Integer destinationId, State state)
            throws ConnectionUnavailableException {
        Sink transport = transports.get(destinationId);
        try {
            transport.publish(payload, transportOptions, state);
        } catch (ConnectionUnavailableException e) {
            transport.setConnected(false);
            strategy.destinationFailed(destinationId);
            log.warn("Failed to publish payload to destination ID " + destinationId + ", " + e.getMessage(), e);
            transport.connectWithRetry();
            throw e;
        }
    }

    @Override
    public void initTransport(OptionHolder sinkOptionHolder, List<OptionHolder> destinationOptionHolders,
                              Map<String, String> deploymentProperties,
                              List<Map<String, String>> destinationDeploymentProperties, Annotation sinkAnnotation,
                              ConfigReader sinkConfigReader, DistributionStrategy strategy, String type,
                              SiddhiAppContext siddhiAppContext) {
        String transportType = sinkOptionHolder.validateAndGetStaticValue(SiddhiConstants.ANNOTATION_ELEMENT_TYPE);
        Extension sinkExtension = DefinitionParserHelper.constructExtension
                (streamDefinition, SiddhiConstants.ANNOTATION_SINK, transportType, sinkAnnotation, SiddhiConstants
                        .NAMESPACE_SINK);

        for (int i = 0; i < destinationOptionHolders.size(); i++) {
            OptionHolder destinationOption = destinationOptionHolders.get(i);
            Sink sink = (Sink) SiddhiClassLoader.loadExtensionImplementation(
                    sinkExtension, SinkExecutorExtensionHolder.getInstance(siddhiAppContext));
            destinationOption.merge(sinkOptionHolder);
            sink.initOnlyTransport(streamDefinition, destinationOption, sinkConfigReader, type,
                    new MultiClientConnectionCallback(transports.size(), strategy),
                    destinationDeploymentProperties.get(i), siddhiAppContext);
            if (!sink.getServiceDeploymentInfoList().isEmpty()) {
                ((ServiceDeploymentInfo) sink.getServiceDeploymentInfoList().get(0)).
                        addDeploymentProperties(deploymentProperties);
            }
            transports.add(sink);
        }
    }


    @Override
    public Class[] getSupportedInputEventClasses() {
        return transports.get(0).getSupportedInputEventClasses();
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * Will be called to connect to the backend before events are published
     *
     * @throws ConnectionUnavailableException if it cannot connect to the backend
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        for (Sink transport : transports) {
            if (!transport.isConnected()) {
                transport.connectWithRetry();
            }
        }
    }

    /**
     * Will be called after all publishing is done, or when ConnectionUnavailableException is thrown
     */
    @Override
    public void disconnect() {
        transports.forEach(Sink::disconnect);
    }

    /**
     * Will be called at the end to clean all the resources consumed
     */
    @Override
    public void destroy() {
        transports.forEach(Sink::destroy);
    }

    @Override
    public List<ServiceDeploymentInfo> getServiceDeploymentInfoList() {
        List<ServiceDeploymentInfo> serviceDeploymentInfoList = new ArrayList<>();
        for (Sink sink : transports) {
            serviceDeploymentInfoList.addAll(sink.getServiceDeploymentInfoList());
        }
        return serviceDeploymentInfoList;
    }

    /**
     * Connection callback to notify DistributionStrategy about new connection initiations and failures
     */
    public class MultiClientConnectionCallback extends DistributedTransport.ConnectionCallback {

        private final int destinationId;
        private final DistributionStrategy strategy;

        private MultiClientConnectionCallback(int destinationId, DistributionStrategy strategy) {
            this.destinationId = destinationId;
            this.strategy = strategy;
        }

        public void connectionEstablished() {
            strategy.destinationAvailable(destinationId);
        }

        public void connectionFailed() {
            strategy.destinationFailed(destinationId);
        }
    }
}

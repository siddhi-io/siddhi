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
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.extension.Extension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements a the distributed sink that could publish to multiple destination using a single
 * client/publisher. Following are some examples,
 * - In a case where there are multiple partitions in a single topic in Kafka, the same kafka client can be used
 * to send events to all the partitions within the topic.
 * - The same email client can send email to different addresses.
 */
public class SingleClientDistributedSink extends DistributedTransport {

    private static final Logger log = LogManager.getLogger(SingleClientDistributedSink.class);
    private Sink sink;
    private int destinationCount = 0;

    @Override
    public void publish(Object payload, DynamicOptions transportOptions, Integer destinationId, State s)
            throws ConnectionUnavailableException {
        try {
            transportOptions.setVariableOptionIndex(destinationId);
            sink.publish(payload, transportOptions, s);
        } catch (ConnectionUnavailableException e) {
            sink.setConnected(false);
            strategy.destinationFailed(destinationId);
            log.warn("Failed to publish payload to destination ID " + destinationId + ".", e);
            sink.connectWithRetry();
            throw e;
        }
    }

    @Override
    public void initTransport(OptionHolder sinkOptionHolder, List<OptionHolder> destinationOptionHolders,
                              Map<String, String> deploymentProperties,
                              List<Map<String, String>> destinationDeploymentProperties,
                              Annotation sinkAnnotation, ConfigReader sinkConfigReader,
                              DistributionStrategy strategy, String type, SiddhiAppContext siddhiAppContext) {
        final String transportType = sinkOptionHolder.validateAndGetStaticValue(SiddhiConstants
                .ANNOTATION_ELEMENT_TYPE);
        Extension sinkExtension = DefinitionParserHelper.constructExtension
                (streamDefinition, SiddhiConstants.ANNOTATION_SINK, transportType, sinkAnnotation, SiddhiConstants
                        .NAMESPACE_SINK);

        Set<String> allDynamicOptionKeys = findAllDynamicOptions(destinationOptionHolders);
        destinationOptionHolders.forEach(optionHolder -> {
            optionHolder.merge(sinkOptionHolder);
            allDynamicOptionKeys.forEach(optionKey -> {
                String optionValue = optionHolder.getOrCreateOption(optionKey, null).getValue();
                if (optionValue == null || optionValue.isEmpty()) {
                    throw new SiddhiAppValidationException("Destination properties can only contain " +
                            "non-empty static values.");
                }

                Option sinkOption = sinkOptionHolder.getOrAddStaticOption(optionKey, optionValue);
                sinkOption.addVariableValue(optionValue);
                destinationCount++;
            });
        });
        this.sink = (Sink) SiddhiClassLoader.loadExtensionImplementation(
                sinkExtension, SinkExecutorExtensionHolder.getInstance(siddhiAppContext));
        this.sink.initOnlyTransport(streamDefinition, sinkOptionHolder, sinkConfigReader, type,
                new SingleClientConnectionCallback(destinationCount, strategy), destinationDeploymentProperties.get(0),
                siddhiAppContext);
        if (!this.sink.getServiceDeploymentInfoList().isEmpty()) {
            ((ServiceDeploymentInfo) this.sink.getServiceDeploymentInfoList().get(0)).
                    addDeploymentProperties(deploymentProperties);
        }
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return sink.getSupportedInputEventClasses();
    }

    /**
     * Will be called to connect to the backend before events are published
     *
     * @throws ConnectionUnavailableException if it cannot connect to the backend
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        sink.connectWithRetry();
    }

    /**
     * Will be called after all publishing is done, or when ConnectionUnavailableException is thrown
     */
    @Override
    public void disconnect() {
        sink.disconnect();
    }

    /**
     * Will be called at the end to clean all the resources consumed
     */
    @Override
    public void destroy() {
        sink.destroy();
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    public List<ServiceDeploymentInfo> getServiceDeploymentInfoList() {
        return sink.getServiceDeploymentInfoList();
    }

    private Set<String> findAllDynamicOptions(List<OptionHolder> destinationOptionHolders) {
        Set<String> dynamicOptions = new HashSet<>();
        destinationOptionHolders.forEach(destinationOptionHolder -> {
            destinationOptionHolder.getDynamicOptionsKeys().forEach(dynamicOptions::add);
            destinationOptionHolder.getStaticOptionsKeys().forEach(dynamicOptions::add);
        });

        return dynamicOptions;
    }

    /**
     * Connection callback to notify DistributionStrategy about new connection initiations and failures
     */
    public class SingleClientConnectionCallback extends DistributedTransport.ConnectionCallback {

        private final int destinations;
        private final DistributionStrategy strategy;

        private SingleClientConnectionCallback(int destinations, DistributionStrategy strategy) {
            this.destinations = destinations;
            this.strategy = strategy;
        }

        public void connectionEstablished() {
            for (int i = 0; i < destinations; i++) {
                strategy.destinationAvailable(i);
            }
        }

        public void connectionFailed() {
            for (int i = 0; i < destinations; i++) {
                strategy.destinationFailed(i);
            }
        }
    }
}

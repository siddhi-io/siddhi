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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

/**
 * Implementation of {@link Sink} which represents in-memory transport. This implementation can send incoming objects
 * to in-memory transport within JVM.
 */
@Extension(
        name = "inMemory",
        namespace = "sink",
        description = "In-memory transport that can communicate with other in-memory transports within the same JVM, " +
                "it" +
                "is assumed that the publisher and subscriber of a topic uses same event schema (stream definition).",
        parameters = @Parameter(name = "topic", type = DataType.STRING, description = "Event will be delivered to all" +
                "the subscribers of the same topic"),
        examples = @Example(
                syntax = "@sink(type='inMemory', @map(type='passThrough'))\n" +
                        "define stream BarStream (symbol string, price float, volume long)",
                description = "In this example BarStream uses inMemory transport which emit the Siddhi " +
                        "events internally without using external transport and transformation."
        )
)
public class InMemorySink extends Sink<State> {
    private static final Logger log = Logger.getLogger(InMemorySink.class);
    private static final String TOPIC_KEY = "topic";
    private Option topicOption;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{TOPIC_KEY};
    }

    @Override
    protected StateFactory<State> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        topicOption = optionHolder.validateAndGetOption(TOPIC_KEY);
        return null;
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        // do nothing
    }

    @Override
    public void disconnect() {
        // do nothing
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State s) throws ConnectionUnavailableException {
        try {
            InMemoryBroker.publish(topicOption.getValue(dynamicOptions), payload);
        } catch (SubscriberUnAvailableException e) {
            onError(payload, e);
        }
    }

}

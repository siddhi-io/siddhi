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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.OptionHolder;
import org.apache.log4j.Logger;

/**
 * Implementation of {@link Source} to receive events through in-memory transport.
 */
@Extension(
        name = "inMemory",
        namespace = "source",
        description = "In-memory source that can communicate with other in-memory sinks within the same JVM, it " +
                "is assumed that the publisher and subscriber of a topic uses same event schema (stream definition).",
        parameters = @Parameter(name = "topic", type = DataType.STRING, description = "Subscribes to sent on the "
                + "given topic."),
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"topic"})
        },
        examples = @Example(
                syntax = "@source(type='inMemory', @map(type='passThrough'))\n" +
                        "define stream BarStream (symbol string, price float, volume long)",
                description = "In this example BarStream uses inMemory transport which passes the received event " +
                        "internally without using external transport."
        )
)
public class InMemorySource extends Source {
    private static final Logger LOG = Logger.getLogger(InMemorySource.class);
    private static final String TOPIC_KEY = "topic";
    private SourceEventListener sourceEventListener;
    private InMemoryBroker.Subscriber subscriber;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<State> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                    String[] requestedTransportPropertyNames, ConfigReader configReader,
                                    SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        String topic = optionHolder.validateAndGetStaticValue(TOPIC_KEY, "input inMemory source");
        this.subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object event) {
                sourceEventListener.onEvent(event, null);
            }

            @Override
            public String getTopic() {
                return topic;
            }
        };
        return null;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {
        InMemoryBroker.subscribe(subscriber);
    }

    @Override
    public void disconnect() {
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public void pause() {
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Override
    public void resume() {
        InMemoryBroker.subscribe(subscriber);
    }

}

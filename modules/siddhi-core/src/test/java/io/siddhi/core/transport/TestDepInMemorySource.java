/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.transport;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.InMemorySource;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.OptionHolder;

/**
 * Implementation of {@link Source} to receive events through in-memory
 * transport.
 */
@Extension(
        name = "testDepInMemory",
        namespace = "source",
        description = "In-memory source that can communicate with other in-memory sinks within the same JVM, it " +
                "is assumed that the publisher and subscriber of a topic uses same event schema (stream definition).",
        parameters = @Parameter(name = "topic", type = DataType.STRING, description = "Subscribes to sent on the "
                + "given topic."),
        examples = @Example(
                syntax = "@source(type='inMemory', @map(type='passThrough'),\n" +
                        "define stream BarStream (symbol string, price float, volume long)",
                description = "In this example BarStream uses inMemory transport which passes the received event " +
                        "internally without using external transport."
        )
)
public class TestDepInMemorySource extends InMemorySource {
    private static final String TOPIC_KEY = "topic";
    protected SourceEventListener sourceEventListener;
    protected InMemoryBroker.Subscriber subscriber;
    private String symbol;
    private String price;
    private String fail;

    @Override
    public StateFactory<State> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                    String[] requestedTransportPropertyNames, ConfigReader configReader,
                                    SiddhiAppContext siddhiAppContext) {
        super.init(sourceEventListener, optionHolder, requestedTransportPropertyNames, configReader, siddhiAppContext);
        symbol = optionHolder.validateAndGetStaticValue("prop1");
        price = optionHolder.validateAndGetStaticValue("prop2");
        fail = optionHolder.validateAndGetStaticValue("fail", "false");
        this.sourceEventListener = sourceEventListener;
        String topic = optionHolder.validateAndGetStaticValue(TOPIC_KEY, "input inMemory source");
        this.subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object event) {
                if (fail.equals("true")) {
                    sourceEventListener.onEvent(event, new String[]{symbol});
                } else {
                    if (requestedTransportPropertyNames.length == 2 &&
                            requestedTransportPropertyNames[0].equals("symbol") &&
                            requestedTransportPropertyNames[1].equals("price")) {
                        sourceEventListener.onEvent(event, new String[]{symbol, price});
                    }
                }
            }

            @Override
            public String getTopic() {
                return topic;
            }
        };
        return null;
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {
        InMemoryBroker.subscribe(subscriber);
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return new ServiceDeploymentInfo(ServiceDeploymentInfo.ServiceProtocol.UDP, 9000, true);
    }
}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.StreamDefinition;

@Extension(
        name = "testAsyncInMemory",
        namespace = "sink",
        description = "In-memory sink for testing connection unavailable use-case",
        parameters = @Parameter(name = "topic", type = DataType.STRING, description = "Event will be delivered to all" +
                "the subscribers of the same topic"),
        examples = @Example(
                syntax = "@sink(type='inMemory', @map(type='passThrough'),\n" +
                        "define stream BarStream (symbol string, price float, volume long)",
                description = "In this example BarStream uses inMemory transport which emit the Siddhi " +
                        "events internally without using external transport and transformation."
        )
)
public class TestAsyncInMemory extends InMemorySink {
    private static final String TOPIC_KEY = "topic";
    public static int numberOfErrorOccurred = 0;
    public static boolean fail;
    public static boolean failRuntime;
    public static boolean failOnce;
    private SiddhiAppContext siddhiAppContext;

    public TestAsyncInMemory() {
        this.failOnce = false;
        this.fail = false;
        this.failRuntime = false;
        this.numberOfErrorOccurred = 0;
    }

    @Override
    protected StateFactory<State> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        optionHolder.validateAndGetOption(TOPIC_KEY);
        super.init(outputStreamDefinition, optionHolder, sinkConfigReader, siddhiAppContext);
        return null;
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        if (fail || failOnce) {
            failOnce = false;
            numberOfErrorOccurred++;
            throw new ConnectionUnavailableException("Connection unavailable during connection");
        }
        super.connect();
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state) {
        siddhiAppContext.getExecutorService().execute(new Publisher(payload, dynamicOptions, state, this));
    }

    private void send(Object payload, DynamicOptions dynamicOptions, State state)
            throws Throwable {
        super.publish(payload, dynamicOptions, state);
    }

    class Publisher implements Runnable {

        private final Object payload;
        private final DynamicOptions dynamicOptions;
        private final State state;
        private final TestAsyncInMemory sink;

        public Publisher(Object payload, DynamicOptions dynamicOptions, State state, TestAsyncInMemory sink) {
            this.payload = payload;
            this.dynamicOptions = dynamicOptions;
            this.state = state;
            this.sink = sink;
        }

        @Override
        public void run() {
            try {
                if (fail || failOnce) {
                    failOnce = false;
                    numberOfErrorOccurred++;
                    throw new ConnectionUnavailableException("Connection unavailable during publishing");
                }
                sink.send(payload, dynamicOptions, state);
            } catch (Throwable e) {
                if (failRuntime) {
                    onError(payload, dynamicOptions, new SiddhiAppRuntimeException(e.getMessage(), e));
                } else {
                    onError(payload, dynamicOptions, new ConnectionUnavailableException(e.getMessage(), e));
                }
            }
        }
    }
}

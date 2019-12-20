/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.transport.DynamicOptions;

@Extension(
        name = "testFailingInMemory",
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
public class TestFailingInMemorySink extends InMemorySink {
    public static int numberOfErrorOccurred = 0;
    public static volatile boolean fail;
    public static boolean failOnce;
    public static boolean publishAlwaysFail = false;

    public TestFailingInMemorySink() {
        this.failOnce = false;
        this.fail = false;
        this.numberOfErrorOccurred = 0;
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
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        if (fail || failOnce || publishAlwaysFail) {
            failOnce = false;
            numberOfErrorOccurred++;
            throw new ConnectionUnavailableException("Connection unavailable during publishing");
        }
        super.publish(payload, dynamicOptions, state);
    }
}

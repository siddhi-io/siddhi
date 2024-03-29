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
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Extension(
        name = "testOptionInMemory",
        namespace = "sink",
        description = "In-memory sink for testing distributed sink in multi client mode. This dummy " +
                "sink simply overrides getSupportedDynamicOptions return nothing so that when distributed " +
                "sink will identify it as a multi-client sink as there are no dynamic options",
        parameters = @Parameter(name = "topic", type = DataType.STRING,
                description = "Event will be delivered to all the subscribers of the same topic"),
        examples = @Example(
                syntax = "@sink(type='testInMemory', @map(type='passThrough'),\n" +
                        "@distribution(strategy='roundRobin',\n" +
                        "@destination(topic = 'topic1'), \n" +
                        "@destination(topic = 'topic2')))\n" +
                        "define stream BarStream (symbol string, price float, volume long);",
                description = "In the following example BarStream uses testInMemory transport which emit the Siddhi " +
                        "events internally without using external transport and transformation."
        )
)
public class TestOptionInMemorySink extends InMemorySink {

    private static final Logger log = LogManager.getLogger(TestOptionInMemorySink.class);
    private String topicPrefix;

    @Override
    protected StateFactory<State> init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                       ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        topicPrefix = (String) siddhiAppContext.getAttributes().get("TopicPrefix");
        topicOption = optionHolder.validateAndGetOption(TOPIC_KEY);
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State s) throws ConnectionUnavailableException {
        try {
            InMemoryBroker.publish(topicPrefix + "-" + topicOption.getValue(dynamicOptions), payload);
        } catch (SubscriberUnAvailableException e) {
            log.error(e.getMessage(), e);
        }
    }
}

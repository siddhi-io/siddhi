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

package org.wso2.siddhi.extension.output.transport.kafka;

import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.NoSuchAttributeException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.extension.output.mapper.text.TextOutputMapper;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.io.Transport;
import org.wso2.siddhi.query.api.execution.io.map.Mapping;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.OutputStream;

public class KafkaOutputTransportTestCase {
    static final Logger log = Logger.getLogger(KafkaOutputTransportTestCase.class);

    @Test(expected = IllegalArgumentException.class)
    public void testPublisherWithKafkaTransport() throws InterruptedException {
        StreamDefinition streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.INT)
                .attribute("volume", Attribute.Type.FLOAT);

        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("kafka")
                        .option("topic", "test")
                        .option("meta.broker.list", "localhost:9092"),
                OutputStream.OutputEventType.CURRENT_EVENTS,
                Mapping.format("json")
        );

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
//        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
//        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
    }
}

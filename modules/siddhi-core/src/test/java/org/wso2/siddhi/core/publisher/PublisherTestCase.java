/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.publisher;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.io.Transport;
import org.wso2.siddhi.query.api.execution.io.map.Mapping;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.OutputStream;

import java.util.Arrays;

public class PublisherTestCase {

    //    from FooStream
    //    select *
    //    publish all events email options (
    //            address  "abc@test.con",
    //            subject  "{{data}}-type")
    //    map text 	“””
    //    Hi user
    //    {{data}} on {{time}}
    //    ”””
    //    ;
    @Test
    public void testPublishEmailWithTextMapping() {
        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("email").
                        option("address", "abc@test.con").option("subject", "{{data}}-type"),
                OutputStream.OutputEventType.ALL_EVENTS,
                Mapping.format("text").map("\n" +
                        "    Hi user\n" +
                        "    {{data}} on {{time}}\n" +
                        "    "));
    }

    @Test
    public void testPublishEmailWithTextMappingCurrentEvents() {
        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("email").
                        option("address", "abc@test.con").option("subject", "{{data}}-type"),
                Mapping.format("text").map("\n" +
                        "    Hi user\n" +
                        "    {{data}} on {{time}}\n" +
                        "    "));

    }

    //    from FooStream
    //    publish inMemory options (topic ‘foo’)
    //    map passThrough;
    @Test
    public void testPublisherWithOutMapping() throws InterruptedException {
        StreamDefinition streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.INT)
                .attribute("volume", Attribute.Type.FLOAT);

        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("inMemory").option("topic", "foo"), OutputStream.OutputEventType.CURRENT_EVENTS,
                Mapping.format("text")
        );

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }

    //    from FooStream
    //    publish inMemory options (topic ‘foo’)
    //    map text """
    //          Hi user
    //          {{data}} on {{time}}
    //          """;
    @Test
    public void testPublisherWithMapping() throws InterruptedException {
        StreamDefinition streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.INT)
                .attribute("volume", Attribute.Type.FLOAT);

        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("inMemory").option("topic", "foo"), OutputStream.OutputEventType.CURRENT_EVENTS,
                Mapping.format("text").body("Hi user {{data}} on {{time}}")
        );

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }

    //    from FooStream
    //    publish inMemory options (topic "foo", symbol "{{symbol}}")
    //    map text """
    //          Hi user
    //          {{data}} on {{time}}
    //          """;
    @Test
    public void testPublisherWithDynamicTransportOptions() throws InterruptedException {
        StreamDefinition streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.INT)
                .attribute("volume", Attribute.Type.FLOAT);

        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.publish(
                Transport.transport("inMemory")
                        .option("topic", "foo")
                        .option("symbol", "{{symbol}}")
                        .option("symbol-price", "{{symbol}}-{{price}}")
                        .option("non-exist-symbol", "{{non-exist}}-{{symbol}}")
                        .option("non-exist", "{{non-exist}}"),
                OutputStream.OutputEventType.CURRENT_EVENTS,
                Mapping.format("text").body("Price of a {{symbol}} share is ${{price}}.")
        );

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }

    //    from FooStream
    //    select *
    //    insert into OutStream;
    @Test
    public void testInsertStream() throws InterruptedException {
        StreamDefinition streamDefinition = StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.INT)
                .attribute("volume", Attribute.Type.FLOAT);
        Query query = Query.query();
        query.from(
                InputStream.stream("FooStream")
        );
        query.insertInto("OutStream");

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(streamDefinition);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.addCallback("OutStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                System.out.println(Arrays.toString(events));
            }
        });

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }
}
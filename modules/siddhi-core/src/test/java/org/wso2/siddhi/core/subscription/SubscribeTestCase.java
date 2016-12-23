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
package org.wso2.siddhi.core.subscription;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.execution.Subscription;
import org.wso2.siddhi.query.api.execution.io.Transport;
import org.wso2.siddhi.query.api.execution.io.map.Mapping;

public class SubscribeTestCase {

    private static final Logger log = Logger.getLogger(SubscribeTestCase.class);

    @Test
    public void testCreatingInmemorySubscription() throws InterruptedException {
        String stream = "define stream FooStream (symbol inputmapper, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory").
                        option("topic", "foo"));

        subscription.map(Mapping.format("passThrough"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testCreatingInmemorySubscriptionWithoutMapping() throws InterruptedException {
        String stream = "define stream FooStream (symbol inputmapper, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory").
                        option("topic", "foo"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
    }

    /**
     * Expected input format:
     * {'symbol': 'WSO2', 'price': 56.75, 'volume': 5, 'country': 'Sri Lanka'}
     */
    @Test
    public void subscriptionTest1() throws InterruptedException {
        log.info("Subscription Test 1: Test an in memory transport with default json mapping");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("json"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void subscriptionTest2() throws InterruptedException {
        log.info("Subscription Test 2: Test an in memory transport with named and positional json mapping - expect " +
                "exception");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        // First two parameters are based on position and the last one is named paramater
        subscription.map(Mapping.format("json").map("$.country").map("$.price").map("$.volume", "volume"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.shutdown();
    }

    /**
     * Expected input format:
     * {'symbol': 'WSO2', 'price': 56.75, 'volume': 5, 'country': 'Sri Lanka'}
     */
    @Test
    public void subscriptionTest3() throws InterruptedException {
        log.info("Subscription Test 3: Test an in memory transport with custom positional json mapping");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("json").map("$.symbol").map("$.price").map("$.volume"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }

    /**
     * Expected input format:
     * {'symbol': 'WSO2', 'price': 56.75, 'volume': 5, 'country': 'Sri Lanka'}
     */
    @Test
    public void subscriptionTest4() throws InterruptedException {
        log.info("Subscription Test 4: Test an in memory transport with custom named json mapping");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("json").map("volume", "$.volume").map("symbol", "$.symbol").map("price", "$" +
                ".price"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void subscriptionTest5() throws InterruptedException {
        log.info("Subscription Test 5: Test infer output stream using json mapping");

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("json").map("volume", "$.volume").map("symbol", "$.symbol").map("price", "$" +
                ".price"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void subscriptionTest6() throws InterruptedException {
        log.info("Subscription Test 6: Test error in infer output stream using json mapping without mapping name");

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("json").map("$.volume").map("$.symbol").map("$.price"));
        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });
    }

    @Test
    public void subscriptionTest7() throws InterruptedException {
        log.info("Subscription Test 7: Test an in memory transport with custom text mapping");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("text").map("regex1[1]").map("regex1[2]").map("regex1[3]").option("regex1", "" +
                "([^,;]+),([^,;]+),([^,;]+),([^,;]+)"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void subscriptionTest8() throws InterruptedException {
        log.info("Subscription Test 8: Test an in memory transport with custom text mapping");

        String stream = "define stream FooStream (symbol string, price float, volume int); ";

        Subscription subscription = Subscription.Subscribe(
                Transport.transport("inMemory"));

        subscription.map(Mapping.format("text").map("regex1[2]").map("regex2[2]").map("regex3[2]")
                .option("regex1", "(symbol=)([^,;]+)")
                .option("regex2", "(price=)([^,;]+)")
                .option("regex3", "(volume=)([^,;]+)"));

        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        Thread.sleep(5000);

        executionPlanRuntime.shutdown();
    }
}

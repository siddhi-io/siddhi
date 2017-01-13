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

package org.wso2.siddhi.extension.input.mapper.map;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.Before;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;
import org.wso2.siddhi.core.util.transport.InMemoryInputTransport;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.Subscription;
import org.wso2.siddhi.query.api.execution.io.Transport;
import org.wso2.siddhi.query.api.execution.io.map.Mapping;

import java.util.HashMap;

public class MapInputMapperTestCase {
    static final Logger log = Logger.getLogger(MapInputMapperTestCase.class);
    private HashMap<String,Object> hashMap = new HashMap<>();

    @Before
    public void init() {
        hashMap.put("volume", 5);
        hashMap.put("symbol", "WSO2");
        hashMap.put("price", 56.75);
        hashMap.put("country", "Sri Lanka");
    }


    @Test
    public void subscriptionTest12() throws InterruptedException {
        log.info("Subscription Test 12: Test an in memory transport with default hashmap mapping");

        Subscription subscription = Subscription.Subscribe(Transport.transport("inMemory").option("topic","stock"));
        subscription.map(Mapping.format("map"));
        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("inputtransport:inMemory", InMemoryInputTransport.class);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        InMemoryBroker.publish("stock",hashMap);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void subscriptionTest13() throws InterruptedException {
        log.info("Subscription Test 13: Test an in memory transport with custom positional hashmap mapping");

        Subscription subscription = Subscription.Subscribe(Transport.transport("inMemory").option("topic","stock"));
        subscription.map(Mapping.format("map").map("symbol").map("price").map("volume"));
        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("output_symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("inputtransport:inMemory", InMemoryInputTransport.class);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        InMemoryBroker.publish("stock",hashMap);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void subscriptionTest14() throws InterruptedException {
        log.info("Subscription Test 14:  Test an in memory transport with custom named hashmap mapping");

        Subscription subscription = Subscription.Subscribe(Transport.transport("inMemory").option("topic","stock"));
        subscription.map(Mapping.format("map").map("output_symbol","symbol").map("output_price","price").map("output_volume","volume"));
        subscription.insertInto("FooStream");

        ExecutionPlan executionPlan = ExecutionPlan.executionPlan();
        executionPlan.defineStream(StreamDefinition.id("FooStream")
                .attribute("output_volume", Attribute.Type.INT)
                .attribute("output_symbol", Attribute.Type.STRING)
                .attribute("output_price", Attribute.Type.FLOAT));
        executionPlan.addSubscription(subscription);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("inputtransport:inMemory", InMemoryInputTransport.class);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.start();

        InMemoryBroker.publish("stock",hashMap);

        executionPlanRuntime.shutdown();
    }
}

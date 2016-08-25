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
package org.wso2.siddhi.extension.graph;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test class for the LargestConnectedComponentProcessor
 */
public class LargestConnectedComponentTestCase {
    private static final Logger log = Logger.getLogger(LargestConnectedComponentTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void LargestConnectedComponentTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id String, friendsId String, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#graph:lcc(id,friendsId,false) " +
                "select largestConnectedComponent " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"1234", "2345", 0});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"2345", "5678", 1});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"5678", "1234", 3});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"5522", "3322", 3});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"3322", "4567", 3});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"4567", "7890", 3});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"7890", "5428", 3});
        Thread.sleep(4000);
        Assert.assertEquals(4, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}

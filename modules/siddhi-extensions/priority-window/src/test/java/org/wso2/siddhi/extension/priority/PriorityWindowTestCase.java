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
package org.wso2.siddhi.extension.priority;

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
 * test cases for PriorityWindowProcessor
 */
public class PriorityWindowTestCase {
    private static final Logger log = Logger.getLogger(PriorityWindowTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void priorityWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id string, priority long, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#priority:priorityWindow(id,priority,5 sec) " +
                "select changedEvents " +
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
        inputHandler.send(new Object[]{"IBM", 2, 0});
        Thread.sleep(1500);
        inputHandler.send(new Object[]{"WSO2", 5, 1});
        Thread.sleep(4500);
        inputHandler.send(new Object[]{"WSO2", 5, 3});
        Thread.sleep(2000);
        Assert.assertEquals(4, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void priorityWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id string, priority long, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#priority:priorityWindow(id,priority,1 sec) " +
                "select changedEvents " +
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
        inputHandler.send(new Object[]{"IBM", 2, 0});
        Thread.sleep(1500);
        inputHandler.send(new Object[]{"WSO2", 5, 1});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 5, 3});
        Thread.sleep(4000);
        Assert.assertEquals(5, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void priorityWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id string, priority long, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#priority:priorityWindow(id,priority,5 sec) " +
                "select changedEvents " +
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
        inputHandler.send(new Object[]{"IBM", 2, 0});
        Thread.sleep(1500);
        inputHandler.send(new Object[]{"WSO2", 5, 1});
        Thread.sleep(4500);
        inputHandler.send(new Object[]{"WSO2", 5, 3});
        Thread.sleep(4000);
        Assert.assertEquals(5, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void priorityWindowTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (id string, priority long, volume int);";
        String query = "" + "@info(name = 'query1') " +
                "from cseEventStream#priority:priorityWindow(id,priority,5 sec) " +
                "select changedEvents " +
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
        inputHandler.send(new Object[]{"IBM", 2, 0});
        Thread.sleep(1500);
        inputHandler.send(new Object[]{"WSO2", 5, 1});
        Thread.sleep(5500);
        inputHandler.send(new Object[]{"WSO2", 5, 3});
        Thread.sleep(1000);
        Assert.assertEquals(5, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }
}

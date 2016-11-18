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
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class PriorityStreamProcessorTestCase {
    private static final Logger log = Logger.getLogger(PriorityStreamProcessorTestCase.class);
    private AtomicInteger eventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        eventCount = new AtomicInteger(0);
        eventArrived = false;
    }

    @Test
    public void priorityTest1() throws InterruptedException {
        log.info("Priority Window test 1: Testing increment and decrement of priority");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    int count = eventCount.addAndGet(inEvents.length);
                    long priority = (Long) inEvents[0].getData(4);
                    if (count == 1) {
                        Assert.assertEquals("Initial priority does not match with input", 1L, priority);
                    } else if (count == 2) {
                        Assert.assertEquals("Priority is not increased by the second event", 4L, priority);
                    } else {
                        Assert.assertEquals("Priority is not increased by time", 6L - count, priority);
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 1L, 0});
        inputHandler.send(new Object[]{"IBM", 3L, 1});
        Thread.sleep(5000);
        Assert.assertEquals(6, eventCount.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void priorityTest2() throws InterruptedException {
        log.info("Priority Window test 2: Sending first event with 0 priority");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventCount.addAndGet(inEvents.length);
                    long priority = (Long) inEvents[0].getData(4);
                    Assert.assertEquals("Initial priority does not match with input", 0L, priority);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 0L, 0});
        Thread.sleep(2000);
        Assert.assertEquals(1, eventCount.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void priorityTest3() throws InterruptedException {
        log.info("Priority Window test 3: Sending event with null key");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{null, 10L, 0});
        Thread.sleep(1000);
        Assert.assertFalse(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void priorityTest4() throws InterruptedException {
        log.info("Priority Window test 4: Sending event with null priority");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 0});
        Thread.sleep(1500);
        Assert.assertFalse(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void priorityTest5() throws InterruptedException {
        log.info("Priority Window test 5: Testing increment and decrement of multiple events");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventCount.addAndGet(inEvents.length);
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 2L, 0});
        inputHandler.send(new Object[]{"WSO2", 1L, 0});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 1L, 1});
        Thread.sleep(2500);
        Assert.assertEquals(7, eventCount.get());
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void priorityTest6() throws InterruptedException {
        log.info("Priority Window test 6: Testing invalid number of arguments");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 1 sec, volume) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void priorityTest7() throws InterruptedException {
        log.info("Priority Window test 7: Testing invalid first parameter");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(2 sec, priority, 1 sec, volume) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void priorityTest8() throws InterruptedException {
        log.info("Priority Window test 8: Testing invalid second parameter");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, 1 milliseconds, 1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void priorityTest9() throws InterruptedException {
        log.info("Priority Window test 9: Testing invalid third parameter");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, volume) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
    }

    @Test
    public void priorityTest10() throws InterruptedException {
        log.info("Priority Window test 10: Testing increment and decrement of multiple events with integer priority");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, priority int, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#priority:time(symbol, priority, 500 milliseconds) " +
                "select * " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventCount.addAndGet(inEvents.length);
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 2, 0});
        inputHandler.send(new Object[]{"WSO2", 1, 0});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 1, 1});
        Thread.sleep(1100);
        Assert.assertEquals(7, eventCount.get());
        executionPlanRuntime.shutdown();
    }
}

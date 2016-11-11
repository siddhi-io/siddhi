/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.query.window;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class TimeWindowTestCase {
    private static final Logger log = Logger.getLogger(TimeWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void timeWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(4000);
        Assert.assertEquals(2, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }


    /**
     * Commenting out intermittent failing test case until fix this properly.
     */

    @Test
    public void timeWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.time(1 sec) select symbol,price," +
                "volume insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        Thread.sleep(4000);
        Assert.assertEquals(6, inEventCount);
        Assert.assertEquals(6, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void timeWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();


        String queries = "define stream fireAlarmEventStream (deviceID string, sonar double);\n" +
                "@info(name = 'query1')\n" +
                "from fireAlarmEventStream#window.time(30 milliseconds)\n" +
                "select deviceID\n" +
                "insert expired events into analyzeStream;\n" +
                "" +
                "@info(name = 'query2')\n" +
                "from analyzeStream\n" +
                "select deviceID\n" +
                "insert into bulbOnStream;\n";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(queries);

        executionPlanRuntime.addCallback("analyzeStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("fireAlarmEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"id1", 20d});
        inputHandler.send(new Object[]{"id2", 20d});
        Thread.sleep(2000);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    /**
     * Testing playback enabled execution plan contains time window with 2 sec interval.
     *
     * @throws InterruptedException
     */
    @Test
    public void timeWindowTest4() throws InterruptedException {
        log.info("TimeWindow Test4: Testing playback without heartbeat");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@Plan:playback " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        timestamp += 2000;
        inputHandler.send(timestamp, new Object[]{"GOOGLE", 0.0f, 1});
        Thread.sleep(100);
        Assert.assertEquals(3, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    /**
     * Testing playback with heartbeat enabled execution plan contains time window with 2 sec interval.
     *
     * @throws InterruptedException
     */
    @Test
    public void timeWindowTest5() throws InterruptedException {
        log.info("TimeWindow Test5: Testing playback with heartbeat enabled");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@Plan:playback(idleTime = '100 millisecond', increment = '2 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});

        Thread.sleep(200);  // Anything more than 100 is enough. Used 200 to be on safe side

        Assert.assertEquals(2, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}

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
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

public class FirstUniqueWindowTestCase {
    private static final Logger log = Logger.getLogger(FirstUniqueWindowTestCase.class);
    private int count;
    private boolean eventArrived;
    private AtomicInteger atomicCount;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
        atomicCount = new AtomicInteger(0);
    }

    @Test
    public void firstUniqueWindowTest1() throws InterruptedException {
        log.info("firstUniqueWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.firstUnique(ip) " +
                "select ip " +
                "insert into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.5"});

        Thread.sleep(1000);

        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("Number of output event value", 3, count);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void firstUniqueWindowTest2() throws InterruptedException {
        log.info("firstUniqueWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.firstUnique(ip) " +
                "select ip " +
                "insert into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.12"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});

        Thread.sleep(1000);

        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("Number of output event value", 2, count);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void UniqueWindowTest3() throws InterruptedException {
        log.info("UniqueWindow test3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.firstUnique(symbol) join twitterStream#window.unique(user) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                    if (inEvents != null) {
                        for (Event inEvent : inEvents) {
                            atomicCount.incrementAndGet();
                        }
                    }
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 59.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{"User2", "Hello World2", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 75.6f, 100});
            SiddhiTestHelper.waitForEvents(100, 2, atomicCount, 10000);
            Assert.assertEquals(2, atomicCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

}

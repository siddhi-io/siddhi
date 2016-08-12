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
package org.wso2.siddhi.core.query.join;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class JoinTestCase {
    private static final Logger log = Logger.getLogger(JoinTestCase.class);
    private AtomicInteger inEventCount;
    private AtomicInteger removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = new AtomicInteger(0);
        removeEventCount = new AtomicInteger(0);
        eventArrived = false;
    }

    @Test
    public void joinTest1() throws InterruptedException {
        log.info("Join test1");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 6000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 6000);
            Assert.assertEquals(2, inEventCount.get());
            Assert.assertEquals(2, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest2() throws InterruptedException {
        log.info("Join test2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) as a join twitterStream#window.time(1 sec) as b " +
                "on a.symbol== b.company " +
                "select a.symbol as symbol, b.tweet, a.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(500);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);
            Assert.assertEquals(2, inEventCount.get());
            Assert.assertEquals(2, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest3() throws InterruptedException {
        log.info("Join test3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(500 milliseconds) as a " +
                "join cseEventStream#window.time(500 milliseconds) as b " +
                "on a.symbol== b.symbol " +
                "select a.symbol as symbol, a.price as priceA, b.price as priceB " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.getAndAdd(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);
            Assert.assertEquals(2, inEventCount.get());
            Assert.assertEquals(2, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest4() throws InterruptedException {
        log.info("Join test4");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) join twitterStream#window.time(2 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            org.junit.Assert.assertTrue("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0)));
                        }
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        for (Event event : removeEvents) {
                            org.junit.Assert.assertTrue("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0)));
                        }
                        removeEventCount.getAndAdd(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            Thread.sleep(1000);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);
            Assert.assertEquals(2, inEventCount.get());
            Assert.assertEquals(2, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest5() throws InterruptedException {
        log.info("Join test5");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(1) join twitterStream#window.length(1) " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            Thread.sleep(500);
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest6() throws InterruptedException {
        log.info("Join test6");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream " +
                "select symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest7() throws InterruptedException {
        log.info("Join test7");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream as a join twitterStream as b " +
                "select a.symbol, twitterStream.tweet, a.price " +
                "insert all events into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void joinTest8() throws InterruptedException {
        log.info("Join test8");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(1) join twitterStream#window.length(1) " +
                "select cseEventStream.symbol as symbol, tweet, price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            Thread.sleep(500);
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest9() throws InterruptedException {
        log.info("Join test9");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream " +
                "select count() as events, symbol " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            Thread.sleep(500);
            Assert.assertFalse(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest10() throws InterruptedException {
        log.info("Join test10");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream join twitterStream#window.length(1) " +
                "select count() as events, symbol " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.getAndAdd(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.getAndAdd(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            Assert.assertEquals("inEventCount", 2, inEventCount.get());
            Assert.assertEquals("removeEventCount", 0, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest11() throws InterruptedException {
        log.info("Join test11");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream unidirectional join twitterStream#window.length(1) " +
                "select count() as events, symbol, tweet " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.getAndAdd(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.getAndAdd(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);
            Assert.assertEquals("inEventCount", 2, inEventCount.get());
            Assert.assertEquals("removeEventCount", 2, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest12() throws InterruptedException {
        log.info("Join test12");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.getAndAdd(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.getAndAdd(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});

            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 60000);
            Assert.assertEquals("inEventCount", 1, inEventCount.get());
            Assert.assertEquals("removeEventCount", 0, removeEventCount.get());
            Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void joinTest13() throws InterruptedException {
        log.info("Join test13");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, symbol string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.symbol " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.start();
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest14() throws InterruptedException {
        log.info("Join test14");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream order (billnum string, custid string, items string, dow string, timestamp long); " +
                "define table dow_items (custid string, dow string, item string) ; " +
                "define stream dow_items_stream (custid string, dow string, item string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from order join dow_items \n" +
                "on order.custid == dow_items.custid \n" +
                "select  dow_items.item\n" +
                "having order.items == \"item1\" \n" +
                "insert into recommendationStream ;" +

                "@info(name = 'query2') " +
                "from dow_items_stream " +
                "insert into dow_items ;" +
                "" +
                "";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler orderStream = executionPlanRuntime.getInputHandler("order");
            InputHandler itemsStream = executionPlanRuntime.getInputHandler("dow_items_stream");
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });
            executionPlanRuntime.start();
            Thread.sleep(100);
            itemsStream.send(new Object[]{"cust1", "bill1", "item1"});
            orderStream.send(new Object[]{"bill1", "cust1", "item1", "dow1", 12323232l});
            Thread.sleep(100);
            Assert.assertEquals("Event Arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest15() throws InterruptedException {
        log.info("Join test15");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream order (billnum string, custid string, items string, dow string, timestamp long); " +
                "define table dow_items (custid string, dow string, item string) ; " +
                "define stream dow_items_stream (custid string, dow string, item string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from order join dow_items \n" +
                "on order.custid == dow_items.custid \n" +
                "select  dow_items.item\n" +
                "having dow_items.item == \"item1\" \n" +
                "insert into recommendationStream ;" +

                "@info(name = 'query2') " +
                "from dow_items_stream " +
                "insert into dow_items ;" +
                "" +
                "";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler orderStream = executionPlanRuntime.getInputHandler("order");
            InputHandler itemsStream = executionPlanRuntime.getInputHandler("dow_items_stream");
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }

            });
            executionPlanRuntime.start();
            Thread.sleep(100);
            itemsStream.send(new Object[]{"cust1", "bill1", "item1"});
            orderStream.send(new Object[]{"bill1", "cust1", "item1", "dow1", 12323232l});
            Thread.sleep(100);
            Assert.assertEquals("Event Arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest16() throws InterruptedException {
        log.info("Join test16");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream order (billnum string, custid string, items string, dow string, timestamp long); " +
                "define table dow_items (custid string, dow string, item string) ; " +
                "define stream dow_items_stream (custid string, dow string, item string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from order join dow_items \n" +
                "on order.custid == dow_items.custid \n" +
                "select  order.custid\n" +
                "having dow_items.item == \"item1\" \n" +
                "insert into recommendationStream ;" +

                "@info(name = 'query2') " +
                "from dow_items_stream " +
                "insert into dow_items ;" +
                "" +
                "";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler orderStream = executionPlanRuntime.getInputHandler("order");
            InputHandler itemsStream = executionPlanRuntime.getInputHandler("dow_items_stream");

            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }

            });
            executionPlanRuntime.start();
            Thread.sleep(100);
            itemsStream.send(new Object[]{"cust1", "bill1", "item1"});
            orderStream.send(new Object[]{"bill1", "cust1", "item1", "dow1", 12323232l});
            Thread.sleep(100);
            Assert.assertEquals("Event Arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void joinTest17() throws InterruptedException {
        log.info("Join test17");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream order (billnum string, custid string, items string, dow string, timestamp long); " +
                "define table dow_items (custid string, dow string, item string) ; " +
                "define stream dow_items_stream (custid string, dow string, item string); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from order join dow_items \n" +
                "select  dow_items.custid\n" +
                "having order.items == \"item1\" \n" +
                "insert into recommendationStream ;" +

                "@info(name = 'query2') " +
                "from dow_items_stream " +
                "insert into dow_items ;" +
                "" +
                "";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler orderStream = executionPlanRuntime.getInputHandler("order");
            InputHandler itemsStream = executionPlanRuntime.getInputHandler("dow_items_stream");
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });
            executionPlanRuntime.start();
            Thread.sleep(100);
            itemsStream.send(new Object[]{"cust1", "bill1", "item1"});
            orderStream.send(new Object[]{"bill1", "cust1", "item1", "dow1", 12323232l});
            Thread.sleep(100);
            Assert.assertEquals("Event Arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }
}

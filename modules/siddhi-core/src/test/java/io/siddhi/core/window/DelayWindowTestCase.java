/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.window;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delay window implementation testcase
 */

public class DelayWindowTestCase {
    private static final Logger log = LogManager.getLogger(DelayWindowTestCase.class);
    private boolean eventArrived;
    private AtomicInteger count = new AtomicInteger(0);
    private Long lastValue;
    private double averageValue;
    private int removeEventCount;
    private int inEventCount;
    private boolean error;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
        lastValue = Long.valueOf(0);
        averageValue = Double.valueOf(0);
        removeEventCount = 0;
        inEventCount = 0;
        error = true;
    }

    @Test(description = "Check if Siddhi App is created successfully when only one parameter of type either " +
            "int or long is specified")
    public void delayWindowTest0() {
        log.info("DelayWindow Test0 : Testing window parameter definition1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String query = "define window eventWindow(symbol string, price int, volume float) delay(1 hour)";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check if Siddhi App Creation fails when more than one parameter is included",
            expectedExceptions = SiddhiAppCreationException.class)
    public void delayWindowTest1() {
        log.info("DelayWindow Test1 : Testing window parameter definition2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String query = "define window eventWindow(symbol string, price int, volume float) delay(2,3) ";
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
        } catch (SiddhiAppCreationException e) {
            error = false;
            AssertJUnit.assertEquals("There is no parameterOverload for 'delay' that matches attribute types " +
                            "'<INT, INT>'. Supported parameter overloads are (<INT|LONG|TIME> window.delay).",
                    e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test(description = "Check if Siddhi App Creation fails when the type of parameter is neither int or long",
            expectedExceptions = SiddhiAppCreationException.class)
    public void delayWindowTest2() {
        log.info("DelayWindow Test2 : Testing window parameter definition3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String query = "define window eventWindow(symbol string, price int, volume float) delay('abc') ";
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
        } catch (SiddhiAppCreationException e) {
            error = false;
            AssertJUnit.assertEquals("There is no parameterOverload for 'delay' that matches attribute types " +
                            "'<STRING>'. Supported parameter overloads are (<INT|LONG|TIME> window.delay).",
                    e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test(description = "Check whether the events are processed when using delay window")
    public void delayWindowTest3() throws InterruptedException {
        log.info("DelayWindow Test3: Testing delay window event processing");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window csEventWindow (symbol string, price float, volume int) delay(2 sec) output all events;";

        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into csEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from csEventWindow " +
                "select symbol,price,volume " +
                "insert all events into outputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
                for (Event event : events) {
                    AssertJUnit.assertTrue(("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0))));
                }
            }
        });

        InputHandler input = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        input.send(new Object[]{"IBM", 700f, 0});
        input.send(new Object[]{"IBM", 750f, 0});
        input.send(new Object[]{"IBM", 800f, 0});
        input.send(new Object[]{"WSO2", 60.5f, 1});

        SiddhiTestHelper.waitForEvents(100, 4, count, 4000);
        AssertJUnit.assertEquals(4, count.get());
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check whether delay window join is working properly")
    public void delayWindowTest4() throws InterruptedException {
        log.info("DelayWindow Test4 : Testing delay window joins");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.delay(1 sec) join twitterStream#window.delay(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    if (events != null) {
                        count.addAndGet(events.length);
                    }
                    AssertJUnit.assertTrue("WSO2".equals(events[0].getData(0)) || "IBM".equals(events[0].getData(0)));
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            twitterStreamHandler.send(new Object[]{"User2", "Hello World2", "IBM"});

            Thread.sleep(1100);
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(100, 2, count, 5000);
            AssertJUnit.assertEquals(2, count.get());
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(description = "Check whether aggregations are done correctly when using delay window ")
    public void delayWindowTest5() {
        log.info("DelayWindow Test5 : Testing delay window for Aggregation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String eventStream = "" +
                "define stream CargoStream (weight int); " +
                "define stream OutputStream(weight int, totalWeight long, averageWeight double); ";
        String query = "" +
                "@info(name='CargoWeightQuery') " +
                "from CargoStream#window.delay(1 sec) " +
                "select weight, sum(weight) as totalWeight, avg(weight) as averageWeight " +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(eventStream + query);

        siddhiAppRuntime.addCallback("CargoWeightQuery", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        StreamCallback callBack = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    lastValue = (Long) event.getData(1);
                    averageValue = (Double) event.getData(2);
                }
            }
        };

        siddhiAppRuntime.addCallback("OutputStream", callBack);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");
        siddhiAppRuntime.start();
        try {
            inputHandler.send(new Object[]{1000});
            Thread.sleep(100);
            inputHandler.send(new Object[]{1500});

            SiddhiTestHelper.waitForEvents(100, 2, count, 3000);
            AssertJUnit.assertEquals(2, inEventCount);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertEquals(Long.valueOf(2500), lastValue);
            AssertJUnit.assertEquals(Double.valueOf(1250), averageValue);

        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(description = "Check whether the events are being actually delayed, for the given time period.")
    public void delayWindowTest6() throws InterruptedException {
        log.info("DelayWindow Test6: Testing delay time ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define window delayWindow(symbol string, volume int) delay(1450);" +
                "define stream inputStream(symbol string, volume int);" +
                "define window timeWindow(symbol string) time(2 sec);";

        String query = "" +
                "@info(name='query1') " +
                "from inputStream " +
                "select symbol, volume " +
                "insert into delayWindow; " +
                "" +
                "@info(name = 'query2') " +
                "from delayWindow " +
                "select symbol, volume " +
                "insert into analyzeStream; " +
                "" +
                "@info(name='query3') " +
                "from inputStream " +
                "select symbol " +
                "insert into timeWindow; " +
                "" +
                "@info(name='query4') " +
                "from analyzeStream join timeWindow " +
                "on analyzeStream.symbol == timeWindow.symbol " +
                "select analyzeStream.symbol " +
                "insert into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
                for (Event event : events) {
                    AssertJUnit.assertTrue(("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0))));
                }
            }
        });

        InputHandler input = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        input.send(new Object[]{"IBM", 700});
        input.send(new Object[]{"WSO2", 750});

        SiddhiTestHelper.waitForEvents(100, 2, count, 5000);
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check if events are persisted when using delay window")
    public void delayWindowTest7() throws InterruptedException {
        log.info("DelayWindow Test7: Testing persistence ");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "" +
                "define window delayWindow(symbol string, volume int) delay(1450);" +
                "define stream inputStream(symbol string, volume int);" +
                "define window timeWindow(symbol string) time(2 sec);";

        String query = "" +
                "@info(name='query1') " +
                "from inputStream " +
                "select symbol, volume " +
                "insert into delayWindow; " +
                "" +
                "@info(name = 'query2') " +
                "from delayWindow " +
                "select symbol, sum(volume) as totalVolume " +
                "insert into analyzeStream; " +
                "" +
                "@info(name='query3') " +
                "from inputStream " +
                "select symbol " +
                "insert into timeWindow; " +
                "" +
                "@info(name='query4') " +
                "from analyzeStream join timeWindow " +
                "on analyzeStream.symbol == timeWindow.symbol " +
                "select analyzeStream.symbol, analyzeStream.totalVolume " +
                "insert into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                for (Event event : events) {
                    AssertJUnit.assertTrue(("IBM".equals(event.getData(0)) || "WSO2".equals(event.getData(0))));
                }
                lastValue = (Long) events[0].getData(1);
            }
        });

        InputHandler input = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        input.send(new Object[]{"IBM", 700});
        input.send(new Object[]{"WSO2", 750});

        SiddhiTestHelper.waitForEvents(100, 2, count, 4000);

        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();

        input = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

        input.send(new Object[]{"WSO2", 600});
        SiddhiTestHelper.waitForEvents(100, 3, count, 4000);

        AssertJUnit.assertEquals(Long.valueOf(2050), lastValue);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void delayWindowTest8() {
        log.info("DelayWindow Test8 : Testing delay window for dinamic value");
        SiddhiManager siddhiManager = new SiddhiManager();
        String eventStream = "" +
                "define stream CargoStream (weight int); " +
                "define stream OutputStream(weight int, totalWeight long, averageWeight double); ";
        String query = "" +
                "@info(name='CargoWeightQuery') " +
                "from CargoStream#window.delay(1/2) " +
                "select weight, sum(weight) as totalWeight, avg(weight) as averageWeight " +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(eventStream + query);
        siddhiAppRuntime.addCallback("CargoWeightQuery", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        StreamCallback callBack = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    lastValue = (Long) event.getData(1);
                    averageValue = (Double) event.getData(2);
                }
            }
        };
        siddhiAppRuntime.addCallback("OutputStream", callBack);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");
        siddhiAppRuntime.start();
        try {
            inputHandler.send(new Object[]{1000});
            inputHandler.send(new Object[]{1500});

            SiddhiTestHelper.waitForEvents(100, 2, count, 3000);
            AssertJUnit.assertEquals(2, inEventCount);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertEquals(Long.valueOf(2500), lastValue);
            AssertJUnit.assertEquals(Double.valueOf(1250), averageValue);

        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

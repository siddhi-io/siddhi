/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.stream;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.UnitTestAppender;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.transport.TestAsyncInMemory;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.transport.InMemoryBroker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.beans.ExceptionListener;
import java.util.concurrent.atomic.AtomicInteger;

public class FaultStreamTestCase {

    private static final Logger log = (Logger) LogManager.getLogger(FaultStreamTestCase.class);
    private volatile AtomicInteger count;
    private volatile AtomicInteger countStream;
    private volatile boolean eventArrived;
    private volatile AtomicInteger failedCount;
    private volatile boolean failedCaught;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
        countStream = new AtomicInteger(0);
        eventArrived = false;
        failedCount = new AtomicInteger(0);
        failedCaught = false;
    }


    @Test
    public void faultStreamTest1() throws InterruptedException {
        log.info("faultStreamTest1-Tests logging by default when fault handling is not configured explicitly.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[custom:fault() > volume] " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count.addAndGet(inEvents.length);
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Error when running faultAdd(). " +
                    "Exception on class 'io.siddhi.core.stream.FaultFunctionExtension'"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);

    }

    @Test(dependsOnMethods = "faultStreamTest1")
    public void faultStreamTest2() throws InterruptedException {
        log.info("faultStreamTest2-Tests logging when fault handling is set to log.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "@OnError(action='log')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[custom:fault() > volume] " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count.addAndGet(inEvents.length);
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Error when running faultAdd(). Exception on " +
                    "class 'io.siddhi.core.stream.FaultFunctionExtension'"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);
    }

    @Test(dependsOnMethods = "faultStreamTest2")
    public void faultStreamTest3() throws InterruptedException {
        log.info("faultStreamTest3-Tests fault handling when it's set to stream. " +
                "No errors would be logged since exceptions are being gracefully handled.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        Logger logger = (Logger) LogManager.getLogger(StreamJunction.class);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[custom:fault() > volume] " +
                "select symbol, price, symbol as sym1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count.addAndGet(inEvents.length);
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);
    }

    @Test(dependsOnMethods = "faultStreamTest3")
    public void faultStreamTest4() throws InterruptedException {
        log.info("faultStreamTest4-Tests fault handling when it's set to stream. " +
                "Events would be available in the corresponding fault stream");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        Logger logger = (Logger) LogManager.getLogger(StreamJunction.class);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[custom:fault() > volume] " +
                "select symbol, price, symbol as sym1 " +
                "insert into outputStream ;" +
                "" +
                "from !cseEventStream " +
                "select * " +
                "insert into faultStream";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("faultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(3) != null);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
    }

    @Test(dependsOnMethods = "faultStreamTest4")
    public void faultStreamTest5() throws InterruptedException {
        log.info("faultStreamTest5-Tests fault handling when it's set to stream. " +
                "Events would be available in the corresponding fault stream");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        Logger logger = (Logger) LogManager.getLogger(StreamJunction.class);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[custom:fault() > volume] " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("!cseEventStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(3) != null);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
    }


    @Test(dependsOnMethods = "faultStreamTest5")
    public void faultStreamTest6() throws InterruptedException {
        log.info("faultStreamTest6-Tests logging by default when fault handling is not configured "
                + "explicitly at sink level during publishing failures.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(0) != null);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Dropping event at Sink 'inMemory' at"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

    }

    @Test(dependsOnMethods = "faultStreamTest6")
    public void faultStreamTest7() throws InterruptedException {
        log.info("faultStreamTest7-Tests fault handling when it's set to log. " +
                "Events would be logged and dropped during publishing failure at Sink");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='inMemory', topic='{{symbol}}', on.error='log', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(0) != null);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Dropping event at Sink 'inMemory' at"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "faultStreamTest7")
    public void faultStreamTest8() throws InterruptedException {
        log.info("faultStreamTest8-Tests fault handling when it's set to wait. " +
                "Thread would be waiting until Sink reconnects.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                EventPrinter.print(new Event[]{(Event) msg});
                count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='inMemory', topic='{{symbol}}', on.error='wait', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(0) != null);
                countStream.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 5f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(2000);
            Assert.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains(
                    "error while connecting Sink 'inMemory' at 'outputStream', will retry every"));
            Assert.assertEquals(count.get(), 0);
            Assert.assertEquals(countStream.get(), 0);
            InMemoryBroker.subscribe(subscriptionIBM);
            Thread.sleep(6000);
            Assert.assertEquals(count.get(), 1);
            Assert.assertEquals(countStream.get(), 1);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }

    @Test(dependsOnMethods = "faultStreamTest8")
    public void faultStreamTest9() throws InterruptedException {
        log.info("faultStreamTest9-Tests fault handling when it's set to stream at Sink but, " +
                "the fault stream is not configured. Events will be logged and dropped.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='inMemory', topic='{{symbol}}', on.error='stream', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(0) != null);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 0f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(500);
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("after consuming events from Stream " +
                    "'outputStream', Subscriber for topic 'IBM' is unavailable. Hence, dropping event"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "faultStreamTest9")
    public void faultStreamTest10() throws InterruptedException {
        log.info("faultStreamTest10-Tests fault handling when it's set to stream at Sink. " +
                "The events will be available in the corresponding fault stream.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);

        Logger loggerSink = (Logger) LogManager.getLogger(Sink.class);
        Logger loggerStreamJunction = (Logger) LogManager.getLogger(StreamJunction.class);
        loggerSink.addAppender(appender);
        loggerStreamJunction.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@OnError(action='stream')" +
                "@sink(type='inMemory', topic='{{symbol}}', on.error='stream', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("!outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(3) != null);
                eventArrived = true;
                count.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 0f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(500);
            AssertJUnit.assertTrue(((UnitTestAppender) loggerSink.getAppenders().
                    get("UnitTestAppender")).getMessages() == null && ((UnitTestAppender) loggerStreamJunction.
                    getAppenders().get("UnitTestAppender")).getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            loggerSink.removeAppender(appender);
            loggerStreamJunction.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(count.get(), 1);
    }

    @Test(dependsOnMethods = "faultStreamTest10")
    public void faultStreamTest11() throws Exception {

        log.info("faultStreamTest11-Tests capturing runtime exceptions by registering an exception " +
                "listener to SiddhiAppRuntime");

        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "define stream outputStream (symbol string, price float);" +
                "\n" +
                "@PrimaryKey('symbol')" +
                "define table cseStoreTable (symbol string, price float);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price " +
                "insert into cseStoreTable ;" +
                "\n" +
                "@info(name = 'query2') " +
                "from cseEventStream " +
                "select symbol, price " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.handleRuntimeExceptionWith(new ExceptionListener() {
            @Override
            public void exceptionThrown(Exception e) {
                failedCaught = true;
                failedCount.incrementAndGet();
            }
        });
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                count.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        try {
            Thread thread = new Thread() {
                @Override
                public void run() {

                    try {
                        inputHandler.send(new Object[]{"IBM", 0f, 100L});
                        inputHandler.send(new Object[]{"IBM", 1f, 200L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(500);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            siddhiAppRuntime.shutdown();
        }
        Assert.assertTrue(eventArrived);
        Assert.assertTrue(failedCaught);
        Assert.assertEquals(count.get(), 2);
        Assert.assertEquals(failedCount.get(), 1);
    }

    @Test(dependsOnMethods = "faultStreamTest11")
    public void faultStreamTest12() throws InterruptedException {
        log.info("faultStreamTest12-Tests fault handling for async when it's set to log. " +
                "Events would be logged and dropped during publishing failure at Sink");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='testAsyncInMemory', topic='{{symbol}}', on.error='log', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(0) != null);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            Thread.sleep(6000);
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Dropping event at Sink 'testAsyncInMemory' at"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "faultStreamTest12")
    public void faultStreamTest13() throws InterruptedException {
        log.info("faultStreamTest13-Tests fault handling when async set to wait. " +
                "Thread would be waiting until Sink reconnects.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                EventPrinter.print(new Event[]{(Event) msg});
                count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        InMemoryBroker.subscribe(subscriptionIBM);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='testAsyncInMemory', topic='{{symbol}}', on.error='wait', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price, symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                countStream.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        TestAsyncInMemory.fail = true;
        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 10f, 100L});
                        Thread.sleep(100);
                        inputHandler.send(new Object[]{"IBM", 11f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(2000);
            Assert.assertEquals(countStream.get(), 1);
            Assert.assertEquals(count.get(), 0);
            Assert.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("error while connecting Sink 'testAsyncInMemory'" +
                    " at 'outputStream', will retry every"));
            TestAsyncInMemory.fail = false;
            Thread.sleep(11000);
            Assert.assertEquals(countStream.get(), 2);
            Assert.assertEquals(count.get(), 2);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }

    @Test(dependsOnMethods = "faultStreamTest13")
    public void faultStreamTest14() throws InterruptedException {
        log.info("faultStreamTest14-Tests fault handling when async set to wait. " +
                "Thread would be waiting until Sink reconnects.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                EventPrinter.print(new Event[]{(Event) msg});
                count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        InMemoryBroker.subscribe(subscriptionIBM);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='testAsyncInMemory', topic='{{symbol}}', on.error='wait', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price, symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                countStream.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        TestAsyncInMemory.failOnce = true;
        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 6f, 100L});
                        inputHandler.send(new Object[]{"IBM", 7f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(11000);
            Assert.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("Connection unavailable during publishing, " +
                    "error while connecting Sink 'testAsyncInMemory' at 'outputStream', will retry"));
            Assert.assertEquals(count.get(), 2);
            Assert.assertEquals(countStream.get(), 2);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }

    @Test(dependsOnMethods = "faultStreamTest14")
    public void faultStreamTest15() throws InterruptedException {
        log.info("faultStreamTest15-Tests fault handling when async set to stream at Sink. " +
                "The events will be available in the corresponding fault stream.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        Logger loggerSink = (Logger) LogManager.getLogger(Sink.class);
        Logger loggerStreamJunction = (Logger) LogManager.getLogger(StreamJunction.class);
        loggerSink.addAppender(appender);
        loggerStreamJunction.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@OnError(action='stream')" +
                "@sink(type='testAsyncInMemory', topic='{{symbol}}', on.error='stream', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("!outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events[0].getData(3) != null);
                eventArrived = true;
                countStream.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 0f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(500);
            AssertJUnit.assertTrue(((UnitTestAppender) loggerSink.getAppenders().
                    get("UnitTestAppender")).getMessages() == null && ((UnitTestAppender) loggerStreamJunction.
                    getAppenders().get("UnitTestAppender")).getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            loggerSink.removeAppender(appender);
            loggerStreamJunction.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(count.get(), 0);
        Assert.assertEquals(countStream.get(), 1);
    }

    @Test(dependsOnMethods = "faultStreamTest15")
    public void faultStreamTest16() throws InterruptedException {
        log.info("faultStreamTest13-Tests fault handling when async set to wait. " +
                "Thread would be waiting until Sink reconnects.");
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        SiddhiManager siddhiManager = new SiddhiManager();

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                EventPrinter.print(new Event[]{(Event) msg});
                count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };
        InMemoryBroker.subscribe(subscriptionIBM);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "\n" +
                "@sink(type='testAsyncInMemory', topic='{{symbol}}', on.error='wait', @map(type='passThrough')) " +
                "define stream outputStream (symbol string, price float, sym1 string);" +
                "\n" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price, symbol as sym1 " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                countStream.incrementAndGet();
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        TestAsyncInMemory.fail = true;
        TestAsyncInMemory.failRuntime = true;
        try {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        inputHandler.send(new Object[]{"IBM", 0f, 100L});
                        inputHandler.send(new Object[]{"IBM", 1f, 100L});
                    } catch (InterruptedException e) {
                    }
                }
            };
            thread.start();
            Thread.sleep(6000);
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("as on.error='wait' does not handle"));
            Assert.assertEquals(count.get(), 0);
            Assert.assertEquals(countStream.get(), 2);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }
}

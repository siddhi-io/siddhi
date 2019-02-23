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
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class FaultStreamTestCase {

    private static final Logger log = Logger.getLogger(CallbackTestCase.class);
    private volatile AtomicInteger count;
    private volatile boolean eventArrived;
    private volatile AtomicInteger failedCount;
    private volatile boolean failedCaught;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
        eventArrived = false;
        failedCount = new AtomicInteger(0);
        failedCaught = false;
    }


    @Test
    public void faultStreamTest1() throws InterruptedException {
        log.info("faultStreamTest1-Tests logging by default when fault handling is not configured explicitly.");

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

        Logger logger = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages().contains("Error when running faultAdd(). Exception on " +
                    "class 'FaultFunctionExtension'"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);

    }

    @Test
    public void faultStreamTest2() throws InterruptedException {
        log.info("faultStreamTest2-Tests logging when fault handling is set to log.");

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

        Logger logger = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages().contains("Error when running faultAdd(). Exception on " +
                    "class 'FaultFunctionExtension'"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);
    }

    @Test
    public void faultStreamTest3() throws InterruptedException {
        log.info("faultStreamTest3-Tests fault handling when it's set to stream. " +
                "No errors would be logged since exceptions are being gracefully handled.");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:fault", FaultFunctionExtension.class);

        String siddhiApp = "" +
                "@OnError(action='stream')" +
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

        Logger logger = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(0, count.get());
        AssertJUnit.assertFalse(eventArrived);

    }

    @Test
    public void faultStreamTest4() throws InterruptedException {
        log.info("faultStreamTest4-Tests fault handling when it's set to stream. " +
                "Events would be available in the corresponding fault stream");

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

        Logger logger = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
    }

    @Test
    public void faultStreamTest5() throws InterruptedException {
        log.info("faultStreamTest5-Tests fault handling when it's set to stream. " +
                "Events would be available in the corresponding fault stream");

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

        Logger logger = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
    }


    @Test
    public void faultStreamTest6() throws InterruptedException {
        log.info("faultStreamTest6-Tests logging by default when fault handling is not configured "
                + "explicitly at sink level during publishing failures.");

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

        Logger logger = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages().contains("Dropping event at Sink 'inMemory' at"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }

    }

    @Test
    public void faultStreamTest7() throws InterruptedException {
        log.info("faultStreamTest7-Tests fault handling when it's set to log. " +
                "Events would be logged and dropped during publishing failure at Sink");

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

        Logger logger = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
        try {
            inputHandler.send(new Object[]{"IBM", 0f, 100L});
            AssertJUnit.assertTrue(appender.getMessages().contains("Dropping event at Sink 'inMemory' at"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void faultStreamTest8() throws InterruptedException {
        log.info("faultStreamTest8-Tests fault handling when it's set to wait. " +
                "Thread would be waiting until Sink reconnects.");

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
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        Logger logger = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        logger.addAppender(appender);
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
            AssertJUnit.assertTrue(appender.getMessages() == null);
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void faultStreamTest9() throws InterruptedException {
        log.info("faultStreamTest9-Tests fault handling when it's set to stream at Sink but, " +
                "the fault stream is not configured. Events will be logged and dropped.");

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

        Logger loggerSink = Logger.getLogger(Sink.class);
        Logger loggerStreamJunction = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        loggerSink.addAppender(appender);
        loggerStreamJunction.addAppender(appender);
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
            AssertJUnit.assertTrue(appender.getMessages().contains("after consuming events from Stream " +
                    "'outputStream', Dropping event at Sink 'inMemory' at 'outputStream' as its still " +
                    "trying to reconnect!"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing.", e);
        } finally {
            loggerSink.removeAppender(appender);
            loggerStreamJunction.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void faultStreamTest10() throws InterruptedException {
        log.info("faultStreamTest10-Tests fault handling when it's set to stream at Sink. " +
                "The events will be available in the corresponding fault stream.");

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

        Logger loggerSink = Logger.getLogger(Sink.class);
        Logger loggerStreamJunction = Logger.getLogger(StreamJunction.class);
        UnitTestAppender appender = new UnitTestAppender();
        loggerSink.addAppender(appender);
        loggerStreamJunction.addAppender(appender);
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
            AssertJUnit.assertTrue(appender.getMessages() == null);
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
}

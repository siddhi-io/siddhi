/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.query.table.cache;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.query.table.util.TestAppender;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheMissTestCase {
    private static final Logger log = Logger.getLogger(CachePreLoadingTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Table with cache INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Table with cache INSERT tests completed ==");
    }

    @Test(description = "cacheMissTestCase1")
    public void cacheMissTestCase1() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"1\"))\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        try {
            AssertJUnit.assertEquals(2, events.length);
        } catch (NullPointerException ignore) {

        }

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("sending results from store table"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from store table"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase2")
    public void cacheMissTestCase2() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"1\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"WSO2\" ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("sending results from store table"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from store table"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase3") // using query api
    public void cacheMissTestCase3() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"FIFO\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"CISCO", 75.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"WSO2\" ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase4") // using find api (join query)
    public void cacheMissTestCase4() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"FIFO\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                " on CheckStockStream.symbol==StockTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 1L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"CISCO", 75.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(100);
        checkStockStream.send(new Object[]{"WSO2"});

//        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable " +
//                "on symbol == \"WSO2\" ");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
//        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
//        Assert.assertEquals(logMessages.contains("sending results from store table"), true);
//        Assert.assertEquals(Collections.frequency(logMessages, "sending results from store table"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase5") // using query api and 2 primary keys
    public void cacheMissTestCase5() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"FIFO\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"CISCO", 75.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"WSO2\" AND price == 55.6f ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase6") // using query api and 2 primary keys & LRu
    public void cacheMissTestCase6() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"CISCO", 75.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"WSO2\" AND price == 55.6f ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase7") // using query api and 1 primary key & LRu
    public void cacheMissTestCase7() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"CISCO", 75.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"WSO2\" ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            String message = String.valueOf(logEvent.getMessage());
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase8") // 1 primary key & LRu & cointains api (in)
    public void cacheMissTestCase8() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "define stream CheckInStockStream (symbol string); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckInStockStream[StockTable.symbol == symbol in StockTable]\n" +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2"});
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }

        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        InputHandler checkInStockStream = siddhiAppRuntime.getInputHandler("CheckInStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(10);
        checkInStockStream.send(new Object[]{"WSO2"});

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase9") // 2 primary keys & LRu & cointains api (in)
    public void cacheMissTestCase9() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "define stream CheckInStockStream (symbol string, price float); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol AND StockTable.price == price AND StockTable.volume == volume;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckInStockStream[(StockTable.symbol == symbol AND StockTable.price == price) in StockTable]\n" +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6f});
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }

        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        InputHandler checkInStockStream = siddhiAppRuntime.getInputHandler("CheckInStockStream");
        siddhiAppRuntime.start();

//        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
//        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(10);
        checkInStockStream.send(new Object[]{"WSO2", 55.6f});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});

//        Event[] events = siddhiAppRuntime.query("" + //todo: how to validate events in cache??
//                "from StockTable ");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(1, events.length);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase10") // 1 primary key & LRu & update func
    public void cacheMissTestCase10() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 66.5f, 3L});
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }

        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

//        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
//        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"WSO2", 66.5f, 3L});

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase11") // 2 primary keys & LRu & update func
    public void cacheMissTestCase11() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LRU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol AND StockTable.price == price);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6f, 3L});
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }

        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

//        deleteStockStream.send(new Object[]{"WSO2", 55.6f, 1L});
//        deleteStockStream.send(new Object[]{"IBM", 75.6f, 2L});
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"WSO2", 55.6f, 3L});

        siddhiAppRuntime.shutdown();
    }
}

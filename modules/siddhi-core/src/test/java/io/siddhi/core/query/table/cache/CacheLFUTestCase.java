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
import io.siddhi.core.query.table.util.TestAppenderToValidateLogsForCachingTests;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheLFUTestCase {
    private static final Logger log = Logger.getLogger(CacheLFUTestCase.class);
    private int inEventCount;
    private boolean eventArrived;
    private int removeEventCount;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        eventArrived = false;
        removeEventCount = 0;
    }

    @Test(description = "cacheLFUTestCase0") // using query api and 2 primary keys & LFU
    public void cacheLFUTestCase0() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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

        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase1") // using query api and 1 primary key & LFU
    public void cacheLFUTestCase1() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase2") // 1 primary key & LFU & cointains api (in)
    public void cacheLFUTestCase2() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "define stream CheckInStockStream (symbol string); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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

    @Test(description = "cacheLFUTestCase3") // 2 primary keys & LFU & cointains api (in)
    public void cacheLFUTestCase3() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "define stream CheckInStockStream (symbol string, price float); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 4L});
        Thread.sleep(10);
        checkInStockStream.send(new Object[]{"WSO2", 55.6f});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});
        Thread.sleep(10);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"APPLE\" AND price == 75.6f ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase4") // 1 primary key & LFU & update func
    public void cacheLFUTestCase4() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"WSO2", 66.5f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"APPLE\" ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase5") // 2 primary keys & LFU & update func
    public void cacheLFUTestCase5() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"WSO2", 55.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"APPLE\" AND price == 75.6f ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase6") // 1 primary key & LFU & update or add func
    public void cacheLFUTestCase6() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
                "@PrimaryKey(\'symbol\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;";

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
                                Assert.assertEquals(event.getData(), new Object[]{"CISCO", 66.5f, 3L});
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"CISCO", 66.5f, 3L});

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase7") // 2 primary keys & LFU & update or add func
    public void cacheLFUTestCase7() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on (StockTable.symbol == symbol AND StockTable.price == price);";

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
                                Assert.assertEquals(event.getData(), new Object[]{"CISCO", 66.5f, 3L});
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"CISCO", 66.5f, 3L});

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase8") // 2 primary keys & LFU & update or add func with update
    public void cacheLFUTestCase8() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on (StockTable.symbol == symbol AND StockTable.price == price);";

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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"APPLE", 75.6f, 2L});
        Thread.sleep(10);
        updateStockStream.send(new Object[]{"WSO2", 55.6f, 3L});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"APPLE\" AND price == 75.6f ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase9")
    public void cacheLFUTestCase9() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        log.info("testTableJoinQuery2 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
                "@PrimaryKey(\'symbol\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream join StockTable " +
                " on CheckStockStream.symbol==StockTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        Thread.sleep(100);
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});
        Thread.sleep(15000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"IBM\" ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 1);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheLFUTestCase10")
    public void cacheLFUTestCase10() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new TestAppenderToValidateLogsForCachingTests();
        final Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        log.info("testTableJoinQuery2 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"2\", cache.policy=\"LFU\"))\n" +
                "@PrimaryKey(\'symbol\', \'price\') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream join StockTable " +
                " on CheckStockStream.symbol==StockTable.symbol AND CheckStockStream.price==StockTable.price " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        Thread.sleep(100);
        checkStockStream.send(new Object[]{"WSO2", 55.6f});
        Thread.sleep(10);
        stockStream.send(new Object[]{"CISCO", 86.6f, 5L});
        Thread.sleep(15000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"IBM\" AND price == 75.6 ");
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
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 1);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 1);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                1);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);

        siddhiAppRuntime.shutdown();
    }
}

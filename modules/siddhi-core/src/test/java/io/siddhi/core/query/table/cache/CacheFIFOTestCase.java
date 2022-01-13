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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheFIFOTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(CacheFIFOTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test(description = "cacheFIFOTestCase0") // using query api
    public void cacheFIFOTestCase0() throws InterruptedException, SQLException {
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
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

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"CISCO\" ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(": ")[1];
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 2);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 2);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 2);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                2);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheFIFOTestCase1") // using find api (join query)
    public void cacheFIFOTestCase1() throws InterruptedException, SQLException {
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
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
                "from CheckStockStream join StockTable " +
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
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"CISCO", "CISCO", 3L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 2);
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
        checkStockStream.send(new Object[]{"CISCO"});

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 2);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 2);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 2);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                2);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheFIFOTestCase2") // using query api and 2 primary keys
    public void cacheFIFOTestCase2() throws InterruptedException, SQLException {
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
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

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == \"CISCO\" AND price == 75.6f ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), true);
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 2);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 2);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache miss. Loading from store"), 2);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache after loading from store"),
                2);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }
}

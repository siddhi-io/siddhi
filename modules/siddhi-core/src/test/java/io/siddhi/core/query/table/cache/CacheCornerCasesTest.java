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
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheCornerCasesTest {
    private static final Logger log = (Logger) LogManager.getLogger(CacheCornerCasesTest.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("== Cache corner cases tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cache corner cases tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testTableJoinQuery1() throws InterruptedException {
        log.info("testTableJoinQuery1 - OUT 2");
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        TestAppenderToValidateLogsForCachingTests appender = config.
                getAppender("TestAppenderToValidateLogsForCachingTests");
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"2\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable as myTable " +
                "on CheckStockStream.symbol == myTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, myTable.symbol as symbol, " +
                "myTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO3", "WSO3", 3L});
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        stockStream.send(new Object[]{"IBM", 75.6f, 2L});
        Thread.sleep(100);
        stockStream.send(new Object[]{"WSO3", 55.6f, 3L});
        Thread.sleep(100);
        stockStream.send(new Object[]{"IB4", 75.6f, 4L});
        Thread.sleep(100);
        checkStockStream.send(new Object[]{"WSO3"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");

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
        Assert.assertEquals(Collections.frequency(logMessages, "store table size is bigger than cache."), 1);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache constraints satisfied. Checking cache"), 1);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "cache hit. Sending results from cache"), 1);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTableJoinQuery1")
    public void testTableJoinQuery2() throws InterruptedException {
        log.info("testTableJoinQuery2 - OUT 2");
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        TestAppenderToValidateLogsForCachingTests appender = config.
                getAppender("TestAppenderToValidateLogsForCachingTests");
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"testStoreCacheEnabledOptimisation\", @Cache(size=\"2\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from CheckStockStream join StockTable as myTable " +
                "on CheckStockStream.symbol == myTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, myTable.symbol as symbol, " +
                "myTable.volume as volume  " +
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
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO3", "WSO3", 3L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 2L});
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

        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        // Three records are hard coded for lookup wso1, wso2 and wso3
        checkStockStream.send(new Object[]{"WSO3"});
        Thread.sleep(1000);
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        checkStockStream.send(new Object[]{"WSO3"});
        Thread.sleep(1000);

        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");

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
        Assert.assertTrue(logMessages.contains("store table size is bigger than cache."));
        Assert.assertTrue(logMessages.contains("sending results from cache after loading from store"));
        Assert.assertEquals(Collections.frequency(logMessages, "cache hit. Sending results from cache"), 2);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }
}

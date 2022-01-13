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
import io.siddhi.core.query.table.util.TestAppenderToValidateLogsForCachingTests;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheMissTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(CacheMissTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Cache miss tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cache miss tests completed ==");
    }


    @Test(description = "cacheMissTestCase0")
    public void cacheMissTestCase0() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"1\", cache.policy=\"FIFO\"))\n" +
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
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), false);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), true);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from store"), 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "cacheMissTestCase1")
    public void cacheMissTestCase1() throws InterruptedException, SQLException {
        final TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCacheMiss\", @Cache(size=\"1\", cache.policy=\"FIFO\"))\n" +
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

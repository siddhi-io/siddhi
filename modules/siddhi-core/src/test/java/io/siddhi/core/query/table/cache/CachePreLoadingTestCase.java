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
import io.siddhi.core.query.table.util.TestAppender;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CachePreLoadingTestCase {
    private static final Logger log = Logger.getLogger(CachePreLoadingTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Table with cache INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Table with cache INSERT tests completed ==");
    }

    @Test // todo not working
    public void insertIntoTableWithCacheTest1() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        //Configure siddhi to insert events data to table only from specific fields of the stream.
        log.info("insertIntoTableWithCacheTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreForCachePreLoading\", @Cache(size=\"10\"))\n" +
                "define table StockTable (symbol string, price float, volume long); ";

//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, volume\n" +
//                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

//        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
//        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
//        Thread.sleep(1000);
//
//        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(streams + query1);
//        siddhiAppRuntime2.start();
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

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
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);

        siddhiAppRuntime.shutdown();
    }
}
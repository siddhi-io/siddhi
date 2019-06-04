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
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheLFUTestCase {
    @Test(description = "cacheMissTestCase6") // using query api and 2 primary keys & LRu
    public void cacheMissTestCase6() throws InterruptedException, SQLException {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
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
        Assert.assertEquals(logMessages.contains("sending results from cache"), true);
        Assert.assertEquals(logMessages.contains("sending results from store table"), false);
        Assert.assertEquals(Collections.frequency(logMessages, "sending results from cache"), 1);

        siddhiAppRuntime.shutdown();
    }

}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.query.table.cache;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class CacheExpireTestCase {
    private static final Logger log = Logger.getLogger(CacheExpireTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Table with cache INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Table with cache INSERT tests completed ==");
    }

    @Test
    public void cacheExpireTestCase1() throws InterruptedException {
        log.info("cacheExpireTestCase1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\", cache.policy=\"FIFO\", " +
                "retention.period=\"1 sec\", purge.interval=\"1 sec\"))\n" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void cacheExpireTestCase2() throws InterruptedException {
        log.info("cacheExpireTestCase2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\", cache.policy=\"FIFO\", " +
                "retention.period=\"1 sec\", purge.interval=\"1 sec\", policy=\"FIFO\"))\n" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
//        AssertJUnit.assertEquals(2, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void cacheExpireTestCase3() throws InterruptedException, SQLException {
        log.info("cacheExpireTestCase3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\", cache.policy=\"FIFO\", " +
                "retention.period=\"1 sec\", purge.interval=\"1 sec\", policy=\"LFU\"))\n" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(200);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
//        AssertJUnit.assertEquals(2, events.length);
        siddhiAppRuntime.shutdown();
    }
}

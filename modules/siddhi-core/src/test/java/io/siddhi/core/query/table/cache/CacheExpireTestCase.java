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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class CacheExpireTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(CacheExpireTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Table with cache INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Table with cache INSERT tests completed ==");
    }

    @Test
    public void cacheExpireTestCase0() throws InterruptedException {
        log.info("cacheExpireTestCase0");
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
        Thread.sleep(1300);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(1300);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(1300);

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"cacheExpireTestCase0"})
    public void cacheExpireTestCase1() throws InterruptedException {
        log.info("cacheExpireTestCase1");
        SiddhiManager siddhiManager2 = new SiddhiManager();
        String streams = "" +
                "define stream StockStream2 (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\", cache.policy=\"LRU\", " +
                "retention.period=\"1 sec\", purge.interval=\"1 sec\"))\n" +
                //"@Index(\"volume\")" +
                "define table StockTable2 (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream2\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable2 ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable2 " +
                "on StockTable2.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream2 = siddhiAppRuntime2.getInputHandler("StockStream2");
        InputHandler deleteStream = siddhiAppRuntime2.getInputHandler("DeleteStream");
        siddhiAppRuntime2.start();

        Event[] events;
        stockStream2.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(1300);
        events = siddhiAppRuntime2.query("" +
                "from StockTable2 ");
        AssertJUnit.assertNull(events);

        stockStream2.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream2.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(1300);
        events = siddhiAppRuntime2.query("" +
                "from StockTable2 ");
        AssertJUnit.assertNull(events);

        stockStream2.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream2.send(new Object[]{"WS2", 55.6f, 5L});
        stockStream2.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(1300);

        events = siddhiAppRuntime2.query("" +
                "from StockTable2 ");
        AssertJUnit.assertNull(events);
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = {"cacheExpireTestCase1"})
    public void cacheExpireTestCase2() throws InterruptedException, SQLException {
        log.info("cacheExpireTestCase2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\", cache.policy=\"LFU\", " +
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
        Thread.sleep(1300);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(1300);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(1300);

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        AssertJUnit.assertNull(events);
        siddhiAppRuntime.shutdown();
    }
}

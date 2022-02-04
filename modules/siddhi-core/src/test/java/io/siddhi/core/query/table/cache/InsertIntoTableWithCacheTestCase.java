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
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class InsertIntoTableWithCacheTestCase {
    private static final Logger log = LogManager.getLogger(InsertIntoTableWithCacheTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Table with cache INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Table with cache INSERT tests completed ==");
    }

    @Test
    public void insertIntoTableWithCacheTest1() throws InterruptedException, SQLException {
        //Configure siddhi to insert events data to table only from specific fields of the stream.
        log.info("insertIntoTableWithCacheTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\"))\n" +
                "define table StockTable (symbol string, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoTableWithCacheTest2() throws InterruptedException, SQLException {
        //Testing table creation with a compound primary key (normal insertion)
        log.info("insertIntoTableWithCacheTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\"))\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 58.6F, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoTableWithCacheTest3() throws InterruptedException, SQLException {
        //Testing table creation with a compound primary key (normal insertion)
        log.info("insertIntoTableWithCacheTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);\n";

        String table = "" +
                "@Store(type=\"testStoreDummyForCache\")\n" + // , @Cache(size="10")
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table StockTable (symbol string, price float, volume long);\n" +
                "@Store(type=\"testStoreDummyForCache\", @Cache(size=\"10\"))\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table MyTable (symbol string, price float);\n";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "select symbol, price " +
                "insert into MyTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + table + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F});
        stockStream.send(new Object[]{"IBM", 75.6F});
        stockStream.send(new Object[]{"MSFT", 57.6F});
        stockStream.send(new Object[]{"WSO2", 58.6F});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from MyTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);

        siddhiAppRuntime.shutdown();
    }
}

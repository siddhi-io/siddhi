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

//    @Test
//    public void insertIntoTableWithCacheTest1() throws InterruptedException, SQLException {
//        //Configure siddhi to insert events data to table only from specific fields of the stream.
//        log.info("insertIntoTableWithCacheTest1");
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String streams = "" +
//                "define stream StockStream (symbol string, price float, volume long); " +
//                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\"))\n" +
//                //"@Index(\"volume\")" +
//                "define table StockTable (symbol string, price float, volume long); ";
//
//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, price, volume\n" +
//                "insert into StockTable ;";
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
//        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
//        siddhiAppRuntime.start();
//
//        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
//        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
//        Thread.sleep(1000);
//
//        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable ");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(2, events.length);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test
//    public void insertIntoTableWithCacheTest2() throws InterruptedException, SQLException {
//        //Configure siddhi to insert events data to table only from specific fields of the stream.
//        log.info("insertIntoTableWithCacheTest1");
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String streams = "" +
//                "define stream StockStream (symbol string, price float, volume long); " +
//                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\"))\n" +
//                //"@Index(\"volume\")" +
//                "define table StockTable (symbol string, price float, volume long); ";
//
//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, price, volume\n" +
//                "insert into StockTable ;";
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
//        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
//        siddhiAppRuntime.start();
//
////        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
////        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
//        Event[] eventsIn = new Event[2];
//        eventsIn[0] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L});
//        eventsIn[1] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L});
//        stockStream.send(eventsIn);
//
//        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable ");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(2, events.length);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test
//    public void insertIntoTableWithCacheTest3() throws InterruptedException, SQLException {
//        //Configure siddhi to insert events data to table only from specific fields of the stream.
//        log.info("insertIntoTableWithCacheTest1");
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String streams = "" +
//                "define stream StockStream (symbol string, price float, volume long); " +
//                "define stream DeleteStream (symbol string); " +
//                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\"))\n" +
//                //"@Index(\"volume\")" +
//                "define table StockTable (symbol string, price float, volume long); ";
//
//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, price, volume\n" +
//                "insert into StockTable ;" +
//                "@info(name = 'query2') " +
//                "from DeleteStream " +
//                "delete StockTable " +
//                "on StockTable.symbol == symbol";
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
//        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
//        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
//        siddhiAppRuntime.start();
//
//        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
//        Thread.sleep(1000);
//        deleteStream.send(new Object[]{"WSO"});
//        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
//        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
//        Thread.sleep(1000);
//        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
//        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
//        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
//        deleteStream.send(new Object[]{"WSO"});
////        deleteStream.send(new Object[]{new Timestamp(System.currentTimeMillis()).getTime()});
////        Thread.sleep(1000);
//
//        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable ");
//        EventPrinter.print(events);
////        AssertJUnit.assertEquals(2, events.length);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test
//    public void insertIntoTableWithCacheTest4() throws InterruptedException, SQLException {
//        //Configure siddhi to insert events data to table only from specific fields of the stream.
//        log.info("insertIntoTableWithCacheTest1");
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String streams = "" +
//                "define stream StockStream (symbol string, price float, volume long, timestamp long); " +
//                "define stream DeleteStream (currentTime long); " +
//                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\"))\n" +
//                //"@Index(\"volume\")" +
//                "define table StockTable (symbol string, price float, volume long, timestamp long); ";
//
//        String query1 = "" +
//                "@info(name = 'query1') " +
//                "from StockStream\n" +
//                "select symbol, price, volume, timestamp\n" +
//                "insert into StockTable ;" +
//                "@info(name = 'query2') " +
//                "from DeleteStream " +
//                "delete StockTable " +
//                "on currentTime - StockTable.timestamp > 1000L";
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
//        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
//        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
//        siddhiAppRuntime.start();
//
//        stockStream.send(new Object[]{"WSO2", 55.6f, 100L, new Timestamp(System.currentTimeMillis()).getTime()});
//        Thread.sleep(1100);
//        stockStream.send(new Object[]{"IBM", 75.6f, 100L, new Timestamp(System.currentTimeMillis()).getTime()});
////        deleteStream.send(new Object[]{"WSO2"});
//        deleteStream.send(new Object[]{new Timestamp(System.currentTimeMillis()).getTime()});
////        Thread.sleep(1000);
//
//        Event[] events = siddhiAppRuntime.query("" +
//                "from StockTable ");
//        EventPrinter.print(events);
////        AssertJUnit.assertEquals(2, events.length);
//
//        siddhiAppRuntime.shutdown();
//    }

    @Test
    public void insertIntoTableWithCacheTest5() throws InterruptedException, SQLException {
        //Configure siddhi to insert events data to table only from specific fields of the stream.
        log.info("insertIntoTableWithCacheTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\", " +
                "expiry.check.interval=\"1 sec\"))\n" +
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
    public void insertIntoTableWithCacheTest6() throws InterruptedException, SQLException {
        //Configure siddhi to insert events data to table only from specific fields of the stream.
        log.info("insertIntoTableWithCacheTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\", " +
                "expiry.check.interval=\"1 sec\", policy=\"FIFO\"))\n" +
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

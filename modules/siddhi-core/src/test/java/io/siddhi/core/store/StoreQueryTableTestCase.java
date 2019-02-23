/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.store;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.StoreQueryCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class StoreQueryTableTestCase {

    private static final Logger log = Logger.getLogger(StoreQueryTableTestCase.class);

    @Test
    public void test1() throws InterruptedException {
        log.info("Test1 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > volume*3/4  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test
    public void test2() throws InterruptedException {
        log.info("Test2 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, volume " +
                "having symbol == 'WSO2' ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test
    public void test3() throws InterruptedException {
        log.info("Test3 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, sum(volume) as totalVolume " +
                "group by symbol " +
                "having totalVolume >150 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(200L, events[0].getData(1));

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, sum(volume) as totalVolume " +
                "group by symbol  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, sum(volume) as totalVolume " +
                "group by symbol,price  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void test4() throws InterruptedException {
        log.info("Test4 table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            Thread.sleep(500);

            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 5 " +
                    "select symbol1, sum(volume) as totalVolume " +
                    "group by symbol " +
                    "having totalVolume >150 ");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(200L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void test5() throws InterruptedException {
        log.info("Test5 table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        try {
            siddhiAppRuntime.start();
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable1 " +
                    "on price > 5 " +
                    "select symbol1, sum(volume) as totalVolume " +
                    "group by symbol " +
                    "having totalVolume >150 ");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(200L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void test6() throws InterruptedException {
        log.info("Test5 table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        try {
            siddhiAppRuntime.start();
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable1 " +
                    "on price > 5 " +
                    "select symbol1, sum(volume)  totalVolume " +
                    "group by symbol " +
                    "having totalVolume >150 ");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(200L, events[0].getData(1));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void test7() throws InterruptedException {
        log.info("Test7 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == 'IBM' " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals("IBM", events[0].getData()[0]);
    }

    @Test
    public void test9() throws InterruptedException {
        log.info("Test9 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, volume " +
                "order by price " +
                "limit 2 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(55.6F, events[0].getData()[1]);
        AssertJUnit.assertEquals(75.6f, events[1].getData()[1]);
    }


    @Test
    public void test10() throws InterruptedException {
        log.info("Test10 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        String storeQuery = "" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, sum(volume) as totalVolume ";
        Event[] events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(200L, events[0].getData()[2]);

        events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(200L, events[0].getData()[2]);
    }

    @Test
    public void test11() throws InterruptedException {
        log.info("Test10 table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        String storeQuery = "" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, sum(volume) as totalVolume " +
                "group by symbol ";
        Event[] events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(100L, events[0].getData()[2]);
        AssertJUnit.assertEquals(100L, events[1].getData()[2]);

        events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(100L, events[0].getData()[2]);
        AssertJUnit.assertEquals(100L, events[1].getData()[2]);
    }

    @Test
    public void test12() throws InterruptedException {
        log.info("Test12 - Test output attributes and its types for table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";
        String storeQuery = "" +
                "from StockTable " +
                "select * ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);

        siddhiAppRuntime.start();
        Attribute[] actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute symbolAttribute = new Attribute("symbol", Attribute.Type.STRING);
        Attribute priceAttribute = new Attribute("price", Attribute.Type.FLOAT);
        Attribute volumeAttribute = new Attribute("volume", Attribute.Type.LONG);
        Attribute[] expectedAttributeArray = new Attribute[]{symbolAttribute, priceAttribute, volumeAttribute};
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);

        storeQuery = "" +
                "from StockTable " +
                "select symbol, sum(volume) as totalVolume ;";

        actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute totalVolumeAttribute = new Attribute("totalVolume", Attribute.Type.LONG);
        expectedAttributeArray = new Attribute[]{symbolAttribute, totalVolumeAttribute};
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);
    }

    @Test
    public void test13() throws InterruptedException {
        log.info("Test13 - Test output attributes and its types for aggregation table");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "define aggregation StockTableAg " +
                "from StockStream " +
                "select symbol, price " +
                "group by symbol " +
                "aggregate every minutes ...year;";
        String storeQuery = "" +
                "from StockTableAg within '2018-**-** **:**:**' per 'minutes' select symbol, price ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);

        siddhiAppRuntime.start();
        Attribute[] actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute symbolAttribute = new Attribute("symbol", Attribute.Type.STRING);
        Attribute priceAttribute = new Attribute("price", Attribute.Type.FLOAT);
        Attribute[] expectedAttributeArray = new Attribute[]{symbolAttribute, priceAttribute};
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);

        storeQuery = "" +
                "from StockTableAg within '2018-**-** **:**:**' per 'minutes' select symbol, sum(price) as total";

        actualAttributeArray = siddhiAppRuntime.getStoreQueryOutputAttributes(SiddhiCompiler.parseStoreQuery
                (storeQuery));
        Attribute totalVolumeAttribute = new Attribute("total", Attribute.Type.DOUBLE);
        expectedAttributeArray = new Attribute[]{symbolAttribute, totalVolumeAttribute};
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertArrayEquals(expectedAttributeArray, actualAttributeArray);
    }

    @Test
    public void test14() throws InterruptedException {
        log.info("Testing InsertOrUpdate store query : 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 300L});
        Thread.sleep(500);

        String storeQuery = "select \"newSymbol\" as symbol, 123.45f as price, 123L as volume " +
                "update or insert into StockTable " +
                "set StockTable.symbol = symbol, StockTable.price=price on StockTable.volume == 100L ";

        siddhiAppRuntime.query(storeQuery);

        Event[] events = siddhiAppRuntime.query("from StockTable select * having volume == 100L;");

        Assert.assertEquals(events.length, 1);
        Assert.assertEquals(events[0].getData()[0], "newSymbol");
        Assert.assertEquals(events[0].getData()[1], 123.45f);
        Assert.assertEquals(events[0].getData()[2], 100L);

        // submit the same store query again to test resetting the query runtime
        siddhiAppRuntime.query(storeQuery);

        events = siddhiAppRuntime.query("from StockTable select * having volume == 100L;");

        Assert.assertEquals(events.length, 1);
        Assert.assertEquals(events[0].getData()[0], "newSymbol");
        Assert.assertEquals(events[0].getData()[1], 123.45f);
        Assert.assertEquals(events[0].getData()[2], 100L);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test15() throws InterruptedException {
        log.info("Testing InsertOrUpdate store query : 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 300L});
        Thread.sleep(500);

        siddhiAppRuntime.query("" +
                "select \"newSymbol\" as symbol, 123.45f as price, 123L as volume " +
                "update or insert into StockTable " +
                "set StockTable.symbol = symbol, StockTable.price=price on StockTable.volume == 500L ");

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(4, allEvents.length);

        Event[] newEvents = siddhiAppRuntime.query("from StockTable select * having volume == 123L;");

        Assert.assertEquals(1, newEvents.length);
        Assert.assertEquals(newEvents[0].getData()[0], "newSymbol");
        Assert.assertEquals(newEvents[0].getData()[1], 123.45f);
        Assert.assertEquals(newEvents[0].getData()[2], 123L);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test16() throws InterruptedException {
        log.info("Testing delete store query : 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        stockStream.send(new Object[]{"GOOGLE", 57.6f, 300L});
        Thread.sleep(500);


        Event[] initialEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(initialEvents.length, 3);

        String storeQuery = "select 100L as vol " +
                "delete StockTable on StockTable.volume == vol;";

        siddhiAppRuntime.query(storeQuery);
        Thread.sleep(500);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 2);

        Event[] events = siddhiAppRuntime.query("from StockTable select * having volume == 100L");
        Assert.assertNull(events);

        // submit the same store query again to test resetting the query runtime
        siddhiAppRuntime.query(storeQuery);

        events = siddhiAppRuntime.query("from StockTable select * having volume == 100L");
        Assert.assertNull(events);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test17() throws InterruptedException {
        log.info("Testing delete store query : 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        stockStream.send(new Object[]{"GOOGLE", 57.6f, 300L});
        Thread.sleep(500);


        Event[] initialEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(initialEvents.length, 3);

        siddhiAppRuntime.query("delete StockTable on StockTable.volume == 100L;");
        Thread.sleep(500);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 2);

        Event[] events = siddhiAppRuntime.query("from StockTable select * having volume == 100L");
        Assert.assertNull(events);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test18() throws InterruptedException {
        log.info("Testing insert store query : 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (id int, symbol string, volume int); " +
                "define table StockTable (id int, symbol string, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{1, "WSO2", 100});
        stockStream.send(new Object[]{2, "IBM", 200});
        stockStream.send(new Object[]{3, "GOOGLE", 300});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(events.length, 3);

        String storeQuery = "select 10 as id, \"YAHOO\" as symbol, 400 as volume insert into StockTable;";

        siddhiAppRuntime.query(storeQuery);
        Thread.sleep(100);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 4);

        Event[] newEvents = siddhiAppRuntime.query("from StockTable select * having id == 10;");
        Assert.assertEquals(newEvents.length, 1);

        Object[] data = newEvents[0].getData();

        Assert.assertEquals(data[0], 10);
        Assert.assertEquals(data[1], "YAHOO");
        Assert.assertEquals(data[2], 400);

        // submit the same store query again to test resetting the query runtime
        siddhiAppRuntime.query(storeQuery);

        newEvents = siddhiAppRuntime.query("from StockTable select * having id == 10;");
        Assert.assertEquals(newEvents.length, 2);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test19() throws InterruptedException {
        log.info("Testing update store query : 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (id int, symbol string, volume int); " +
                "define table StockTable (id int, symbol string, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{1, "WSO2", 100});
        stockStream.send(new Object[]{2, "IBM", 200});
        stockStream.send(new Object[]{3, "GOOGLE", 300});
        Thread.sleep(500);

        String storeQuery = "update StockTable set StockTable.symbol=\"MICROSOFT\", StockTable.volume=2000" +
                " on StockTable.id==2;";
        siddhiAppRuntime.query(storeQuery);
        Thread.sleep(100);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 3);

        Event[] updatedEvents = siddhiAppRuntime.query("from StockTable select * having id == 2");
        Assert.assertEquals(updatedEvents.length, 1);

        Object[] data = updatedEvents[0].getData();

        Assert.assertEquals(data[0], 2);
        Assert.assertEquals(data[1], "MICROSOFT");
        Assert.assertEquals(data[2], 2000);

        // submit the same store query again to test resetting the query runtime
        siddhiAppRuntime.query(storeQuery);
        updatedEvents = siddhiAppRuntime.query("from StockTable select * having id == 2");
        Assert.assertEquals(updatedEvents.length, 1);

        data = updatedEvents[0].getData();

        Assert.assertEquals(data[0], 2);
        Assert.assertEquals(data[1], "MICROSOFT");
        Assert.assertEquals(data[2], 2000);


        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test20() throws InterruptedException {
        log.info("Testing update store query : 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (id int, symbol string, volume int); " +
                "define table StockTable (id int, symbol string, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{1, "WSO2", 100});
        stockStream.send(new Object[]{2, "IBM", 200});
        stockStream.send(new Object[]{3, "GOOGLE", 300});
        Thread.sleep(500);

        siddhiAppRuntime.query("select \"MICROSOFT\" as newSymbol, 2000 as newVolume " +
                "update StockTable " +
                "set StockTable.symbol=newSymbol, StockTable.volume=newVolume " +
                "on StockTable.id==2;");
        Thread.sleep(100);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 3);

        Event[] updatedEvents = siddhiAppRuntime.query("from StockTable select * having id == 2");
        Assert.assertEquals(updatedEvents.length, 1);

        Object[] data = updatedEvents[0].getData();

        Assert.assertEquals(data[0], 2);
        Assert.assertEquals(data[1], "MICROSOFT");
        Assert.assertEquals(data[2], 2000);

        siddhiAppRuntime.shutdown();
    }
}

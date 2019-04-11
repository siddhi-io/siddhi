/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.aggregation;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.StoreQueryCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Aggregation2TestCase {

    private static final Logger LOG = Logger.getLogger(Aggregation2TestCase.class);
    private AtomicInteger inEventCount;
    private AtomicInteger removeEventCount;
    private boolean eventArrived;
    private List<Object[]> inEventsList;
    private List<Object[]> removeEventsList;

    @BeforeMethod
    public void init() {
        inEventCount = new AtomicInteger(0);
        removeEventCount = new AtomicInteger(0);
        eventArrived = false;
        inEventsList = new ArrayList<>();
        removeEventsList = new ArrayList<>();
    }

    @Test
    public void incrementalStreamProcessorTest47() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest47 - Aggregation external timestamp minute granularity");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query = "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as totalPrice, avg(price) as avgPrice " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});

            // Thursday, June 1, 2017 4:05:51 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 500f, null, 200L, 7, 1496289951011L});

            // Thursday, June 1, 2017 4:05:53 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289953000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 96, 1496289953000L});

            Thread.sleep(1000);
            Event[] events = null;
            int i = 0;

            while (events == null && i < 10) {
                events = siddhiAppRuntime.query("from stockAggregation within 0L, 1543664151000L per " +
                        "'minutes' select AGG_TIMESTAMP, symbol, totalPrice, avgPrice ");
                Thread.sleep(100);
                i++;
            }

            AssertJUnit.assertNotNull("Aggregation event list is null", events);
            if (events != null) {
                AssertJUnit.assertEquals("Check time windows", 2, events.length);

                List<Object[]> eventsList = new ArrayList<>();
                for (Event event : events) {
                    eventsList.add(event.getData());
                }


                List<Object[]> expected = Arrays.asList(
                        new Object[]{1496289900000L, "WSO2", 650.0, 216.66666666666666},
                        new Object[]{1496289900000L, "IBM", 1500.0, 375.0}
                );
                AssertJUnit.assertEquals("Data Matched", true,
                        SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected));
            }
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest47"})
    public void incrementalStreamProcessorTest48() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest48 - Aggregation external timestamp second granularity");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query = "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as totalPrice " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});

            // Thursday, June 1, 2017 4:05:51 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 500f, null, 200L, 7, 1496289951011L});

            // Thursday, June 1, 2017 4:05:53 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289953000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 96, 1496289953000L});

            Thread.sleep(1000);

            Event[] events = siddhiAppRuntime.query("from stockAggregation within 0L, 1543664151000L per " +
                    "'seconds' select AGG_TIMESTAMP, symbol, totalPrice ");
            Assert.assertNotNull(events);
            AssertJUnit.assertEquals("Check time windows", 7, events.length);
            List<Object[]> eventsList = new ArrayList<>();
            for (Event event : events) {
                eventsList.add(event.getData());
            }

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, "WSO2", 50.0},
                    new Object[]{1496289950000L, "IBM", 900.0},
                    new Object[]{1496289951000L, "IBM", 100.0},
                    new Object[]{1496289951000L, "WSO2", 500.0},
                    new Object[]{1496289952000L, "IBM", 400.0},
                    new Object[]{1496289953000L, "IBM", 100.0},
                    new Object[]{1496289953000L, "WSO2", 100.0}
            );

            AssertJUnit.assertEquals("Data Matched", true,
                    SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest48"})
    public void incrementalStreamProcessorTest49() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest49 - Aggregate on system timestamp and retrieval on non root duration");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp string);";
        String query = "" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; " +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "within startTime " +
                "per perValue " +
                "select avgPrice, totalPrice as sumPrice, lastTradeValue  " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : removeEvents) {
                            removeEventsList.add(event.getData());
                            removeEventCount.incrementAndGet();
                        }
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM (add 5.30 to get corresponding IST time)
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, "2017-06-01 04:05:50"});

            // Thursday, June 1, 2017 4:05:51 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 50f, 60f, 90L, 6, "2017-06-01 04:05:51"});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, "2017-06-01 04:05:52"});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, "2017-06-01 04:05:52"});

            // Thursday, June 1, 2017 4:05:50 AM (out of order. must be processed with 1st event for 50th second)
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, "2017-06-01 04:05:50"});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, "2017-06-01 04:05:54"});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, "2017-06-01 04:05:54"});

            // Thursday, June 1, 2017 4:05:50 AM (out of order. should not be processed since events for 50th second is
            // no longer in the buffer and @IgnoreEventsOlderThanBuffer is true)
            stockStreamInputHandler.send(new Object[]{"IBM", 50f, 60f, 90L, 6, "2017-06-01 04:05:50"});

            Thread.sleep(1000);
            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, "2017-06-01 04:05:56"});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, "2017-06-01 04:05:56"});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, "2017-06-01 04:06:56"});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, "2017-06-01 04:07:56"});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 700f, null, 200L, 20, "2017-06-01 05:07:56"});

            Thread.sleep(100);
            LocalDate currentDate = LocalDate.now();
            String year = String.valueOf(currentDate.getYear());
            inputStreamInputHandler.send(new Object[]{"IBM", 1, year + "-**-** **:**:**",
                    "2019-06-01 09:35:52 +05:30", "years"});
            Thread.sleep(100);

            List<Object[]> expected = new ArrayList<>();
            expected.add(new Object[]{283.0769230769231, 3680.0, 14000f});

            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 60000);
            AssertJUnit.assertEquals("In events matched", true,
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
            AssertJUnit.assertEquals("Number of success events", 1, inEventCount.get());
            AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest49"},
            expectedExceptions = StoreQueryCreationException.class)
    public void incrementalStreamProcessorTest50() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest50 - Retrieval query syntax validating ");

        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "group by symbol " +
                "aggregate by timestamp every sec...hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.query("from stockAggregation  select * ");
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest50"})
    public void incrementalStreamProcessorTest51() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest51 - Checking aggregation values when queried twice (non external " +
                "timestamp)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);";
        String query = "" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; " +

                "define stream inputStream (symbol string, value int, startTime long, " +
                "endTime long, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "within startTime, endTime " +
                "per perValue " +
                "select AGG_TIMESTAMP, avgPrice, totalPrice as sumPrice " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : removeEvents) {
                            removeEventsList.add(event.getData());
                            removeEventCount.incrementAndGet();
                        }
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});

            stockStreamInputHandler.send(new Object[]{"IBM", 50f, 60f, 90L, 6});

            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16});

            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96});

            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7});

            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9});
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6});

            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6});

            stockStreamInputHandler.send(new Object[]{"IBM", 700f, null, 200L, 20});

            Thread.sleep(100);
            LocalDate currentDate = LocalDate.now();
            String year = String.valueOf(currentDate.getYear() + 2);
            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:52 +05:30",
                    year + "-06-01 09:35:52 +05:30", "hours"});
            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:52 +05:30",
                    year + "-06-01 09:35:52 +05:30", "hours"});
            Thread.sleep(1000);

            Event[] events1 =
                    siddhiAppRuntime.query("from stockAggregation within 0L, " + (System.currentTimeMillis()
                            + 1000000) + "L per 'hours' select AGG_TIMESTAMP, avgPrice, totalPrice as sumPrice");

            Event[] events2 =
                    siddhiAppRuntime.query("from stockAggregation within 0L, " + (System.currentTimeMillis()
                            + 1000000) + "L per 'hours' select AGG_TIMESTAMP, avgPrice, totalPrice as sumPrice");

            Assert.assertNotNull(events1);
            Assert.assertNotNull(events2);

            List<Object[]> firstJoinEvent = new ArrayList<>();
            List<Object[]> secondJoinEvent = new ArrayList<>();
            firstJoinEvent.add(inEventsList.get(0));
            secondJoinEvent.add(inEventsList.get(1));

            List<Object[]> storeQueryEvents1 = new ArrayList<>();
            storeQueryEvents1.add(events1[0].getData());

            List<Object[]> storeQueryEvents2 = new ArrayList<>();
            storeQueryEvents2.add(events2[0].getData());

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);

            AssertJUnit.assertEquals("Event arrived", true, eventArrived);
            AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
            AssertJUnit.assertEquals("In events matched", true,
                    SiddhiTestHelper.isEventsMatch(firstJoinEvent, secondJoinEvent));
            AssertJUnit.assertEquals("Store Query events matched", true,
                    SiddhiTestHelper.isEventsMatch(storeQueryEvents1, storeQueryEvents2));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest51"}, expectedExceptions =
            SiddhiAppCreationException.class)
    public void incrementalStreamProcessorTest52() {
        LOG.info("incrementalStreamProcessorTest52 - Checking partitionbyid parsing");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);\n";
        String query = "@PartitionById " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest52"}, expectedExceptions =
            SiddhiAppCreationException.class)
    public void incrementalStreamProcessorTest53() {
        LOG.info("incrementalStreamProcessorTest53 - Checking partitionbyid enable=true");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);\n";
        String query = "@PartitionById(enable='true') " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest53"})
    public void incrementalStreamProcessorTest54() {
        LOG.info("incrementalStreamProcessorTest54 - Checking partitionbyid enable= false");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);\n";
        String query = "@PartitionById(enable='false') " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest54"}, expectedExceptions =
            SiddhiAppCreationException.class)
    public void incrementalStreamProcessorTest55() {

        Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put("partitionById", "true");
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(null, null, propertiesMap);

        LOG.info("incrementalStreamProcessorTest55 - Checking @partitionbyid overriding");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);\n";
        String query = "@PartitionById(enable='false') " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
    }


    @Test(dependsOnMethods = {"incrementalStreamProcessorTest55"}, expectedExceptions =
            SiddhiAppCreationException.class)
    public void incrementalStreamProcessorTest56() {

        Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put("partitionById", "true");
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(null, null, propertiesMap);

        LOG.info("incrementalStreamProcessorTest55 - Checking partitionbyid system param overriding");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);\n";
        String query = "" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.start();
    }


}


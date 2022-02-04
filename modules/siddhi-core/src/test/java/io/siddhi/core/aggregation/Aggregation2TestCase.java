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
import io.siddhi.core.UnitTestAppender;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.StoreQueryCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.ConfigManager;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
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

    private static final Logger LOG = (Logger) LogManager.getLogger(Aggregation2TestCase.class);
    private boolean eventArrived;
    private AtomicInteger inEventCount;
    private List<Object[]> inEventsList;

    @BeforeMethod
    public void init() {

        inEventCount = new AtomicInteger(0);
        eventArrived = false;
        inEventsList = new ArrayList<>();
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

            Thread.sleep(2000);
            Event[] events = null;
            int i = 0;

            while (events == null && i < 10) {
                events = siddhiAppRuntime.query("from stockAggregation within 0L, 1543664151000L per " +
                        "'minutes' select AGG_TIMESTAMP, symbol, totalPrice, avgPrice ");
                Thread.sleep(100);
                i++;
            }

            AssertJUnit.assertNotNull("Aggregation event list is null", events);
            AssertJUnit.assertEquals("Check time windows", 2, events.length);

            List<Object[]> eventsList = new ArrayList<>();
            for (Event event : events) {
                eventsList.add(event.getData());
            }

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289900000L, "WSO2", 650.0, 216.66666666666666},
                    new Object[]{1496289900000L, "IBM", 1500.0, 375.0}
            );

            AssertJUnit.assertTrue("Data Matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected));

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

            Thread.sleep(2000);

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

            AssertJUnit.assertTrue("Data Matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected));
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

            Thread.sleep(2000);
            LocalDate currentDate = LocalDate.now();
            String year = String.valueOf(currentDate.getYear());
            inputStreamInputHandler.send(new Object[]{"IBM", 1, year + "-**-** **:**:**",
                    "2019-06-01 09:35:52 +05:30", "years"});
            Thread.sleep(100);

            List<Object[]> expected = new ArrayList<>();
            expected.add(new Object[]{283.0769230769231, 3680.0, 14000f});

            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 60000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 1, inEventCount.get());
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest49"},
            expectedExceptions = StoreQueryCreationException.class)
    public void incrementalStreamProcessorTest50() {

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

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isEventsMatch(firstJoinEvent, secondJoinEvent));
            AssertJUnit.assertTrue("On-demand query events matched",
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

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest56"})
    public void incrementalStreamProcessorTest57() throws InterruptedException {
        LOG.info("Check interrupted exception being thrown when SiddhiRuntime has already been shutdown");
        Logger logger = (Logger) LogManager.getLogger(Scheduler.class);

        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = config.getAppender("UnitTestAppender");
        logger.addAppender(appender);
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
        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();
        Thread eventSenderThread = new Thread(() -> {
            try {
                stockStreamInputHandler.send(new Event[]{
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L})});
                stockStreamInputHandler.send(new Event[]{
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L})});
                stockStreamInputHandler.send(new Event[]{
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L}),
                        new Event(System.currentTimeMillis(),
                                new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L})});
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
            }
        }, "EventSenderThread");
        eventSenderThread.start();
        Thread siddhiRuntimeShutdownThread = new Thread(siddhiAppRuntime::shutdown, "SiddhiRuntimeShutdownThread");
        siddhiRuntimeShutdownThread.start();
        Thread.sleep(10000L);
        if (appender.getMessages() != null) {
            AssertJUnit.assertFalse(appender.getMessages().contains("Error when adding time:"));
        }
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest57"})
    public void incrementalStreamProcessorTest5SGTTimeZoneHour() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Monday December 31, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1577721601000L});

        // Tuesday December 31, 2019 00:59:59 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1577725199000L});

        // Tuesday December 31, 2019 01:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1577725201000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2019-**-** **:**:**\" " +
                "per \"hour\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1577721600000L, "WSO2", 60.0},
                new Object[]{1577725200000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneHour"})
    public void incrementalStreamProcessorTest5SGTTimeZoneHour2() throws InterruptedException {
        //feb 29 leap end hour
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday February 29, 2020 23:00:01 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1582988401000L});

        // Saturday February 29, 2020 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1582991999000L});

        // Sunday March 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1582992001000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2020-**-** **:**:**\" " +
                "per \"hour\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1582988400000L, "WSO2", 60.0},
                new Object[]{1582992000000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneHour2"})
    public void incrementalStreamProcessorTest5SGTTimeZoneDay() throws InterruptedException {
        //normal day
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every day ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday February 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1580486401000L});

        // Saturday February 01, 2020 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1580572799000L});

        // Sunday February 02, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1580572801000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2020-**-** **:**:**\" " +
                "per \"day\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1580486400000L, "WSO2", 60.0},
                new Object[]{1580572800000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneDay"})
    public void incrementalStreamProcessorTest5SGTTimeZoneDay2() throws InterruptedException {
        //feb 29 leap
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every day ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday February 29, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1582905601000L});

        // Saturday February 29, 2020 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1582991999000L});

        // Sunday March 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1582992001000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2020-**-** **:**:**\" " +
                "per \"day\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1582905600000L, "WSO2", 60.0},
                new Object[]{1582992000000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneDay2"})
    public void incrementalStreamProcessorTest5SGTTimeZoneDay3() throws InterruptedException {
        //feb 28 non-leap
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every day ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday February 28, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1551283201000L});

        // Saturday February 28, 2019 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1551369599000L});

        // Sunday March 01, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1551369601000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2019-**-** **:**:**\" " +
                "per \"day\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1551283200000L, "WSO2", 60.0},
                new Object[]{1551369600000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneDay3"})
    public void incrementalStreamProcessorTest5SGTTimeZoneDay4() throws InterruptedException {
        //jan 31
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every day ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday January 31, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1548864001000L});

        // Saturday January 31, 2019 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1548950399000L});

        // Sunday February 01, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1548950401000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2019-**-** **:**:**\" " +
                "per \"day\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1548864000000L, "WSO2", 60.0},
                new Object[]{1548950400000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneDay4"})
    public void incrementalStreamProcessorTest5SGTTimeZoneDay5() throws InterruptedException {
        //dec 31 2019
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every day ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Monday December 31, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1577721601000L});

        // Tuesday December 31, 2019 23:59:59 (pm in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1577807999000L});

        // Wednesday January 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1577808001000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2019-**-** **:**:**\" " +
                "per \"day\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1577721600000L, "WSO2", 60.0},
                new Object[]{1577808000000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneDay5"})
    public void incrementalStreamProcessorTest5SGTTimeZoneMonth() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, systemConfigs);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every month ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
//        siddhiAppRuntime.

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Saturday February 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1580486401000L});

        // Saturday February 29, 2020 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1582991999000L});

        // Sunday March 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1582992001000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2020-**-** **:**:**\" " +
                "per \"month\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1580486400000L, "WSO2", 60.0},
                new Object[]{1582992000000L, "WSO2", 80.0}
        );
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneMonth"})
    public void incrementalStreamProcessorTest5SGTTimeZoneYear() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("aggTimeZone", "Asia/Singapore");
        ConfigManager configManager = new InMemoryConfigManager(null, null, configMap);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every year ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
//        siddhiAppRuntime.

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // Tuesday January 01, 2019 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 1546272001000L});

        // Tuesday December 31, 2019 23:59:59 (pm) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, 1577807999000L});

        // Wednesday January 01, 2020 00:00:01 (am) in time zone Asia/Singapore (+08)
        stockStreamInputHandler.send(new Object[]{"WSO2", 80f, 1577808001000L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2018-**-** **:**:**\" " +
                "per \"year\"");
        EventPrinter.print(events);

        Assert.assertNotNull(events, "Queried results cannot be null.");
        AssertJUnit.assertEquals(1, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = new ArrayList<>();
        expected.add(new Object[]{1546272000000L, "WSO2", 60.0});

        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"incrementalStreamProcessorTest5SGTTimeZoneYear"}, expectedExceptions =
            SiddhiAppCreationException.class)
    public void incrementalStreamProcessorTestInvalidTimeZone() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("aggTimeZone", "Asia/Singapo");
        ConfigManager configManager = new InMemoryConfigManager(null, null, configMap);
        siddhiManager.setConfigManager(configManager);

        String stockStream =
                "define stream stockStream (symbol string, price float, timestamp long);";
        String query = " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice  " +
                "group by symbol " +
                "aggregate by timestamp every year ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        siddhiAppRuntime.shutdown();
    }
}


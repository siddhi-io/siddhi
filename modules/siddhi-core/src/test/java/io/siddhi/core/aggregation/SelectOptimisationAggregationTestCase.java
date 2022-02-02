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

package io.siddhi.core.aggregation;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectOptimisationAggregationTestCase {

    private static final Logger LOG = LogManager.getLogger(SelectOptimisationAggregationTestCase.class);
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
    public void aggregationFunctionTestcase1() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase1 - count w/o group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select count() as count " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select count " +
                "order by AGG_TIMESTAMP " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});
            Thread.sleep(1000);

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            // AGG_TIMESTAMP cannot be asserted as it is based on the time test case is run
            List<Object[]> expected = Arrays.asList(
                    new Object[]{2L},
                    new Object[]{2L},
                    new Object[]{2L},
                    new Object[]{2L},
                    new Object[]{1L},
                    new Object[]{1L},
                    new Object[]{1L}
            );
            SiddhiTestHelper.waitForEvents(100, 7, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 7, inEventCount.get());
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase1")
    public void aggregationFunctionTestcase2() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase1 - external timestamp count w/o group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select count() as count " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select AGG_TIMESTAMP, count " +
                "order by AGG_TIMESTAMP " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, 2L},
                    new Object[]{1496289952000L, 2L},
                    new Object[]{1496289954000L, 2L},
                    new Object[]{1496289956000L, 2L},
                    new Object[]{1496290016000L, 1L},
                    new Object[]{1496290076000L, 1L},
                    new Object[]{1496293676000L, 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 7, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 7, inEventCount.get());
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase2")
    public void aggregationFunctionTestcase3() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase2 - count w/o group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, count() as count " +
                "group by symbol " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select AGG_TIMESTAMP, s.symbol, s.count " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, "WSO2", 2L},
                    new Object[]{1496289952000L, "WSO2", 2L},
                    new Object[]{1496289954000L, "IBM", 2L},
                    new Object[]{1496289956000L, "IBM", 2L},
                    new Object[]{1496290016000L, "IBM", 1L},
                    new Object[]{1496290076000L, "IBM", 1L},
                    new Object[]{1496293676000L, "CISCO", 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 7, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 7, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase3")
    public void aggregationFunctionTestcase4() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase2 - count with same group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, count() as count " +
                "group by symbol " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select s.symbol, sum(count) as count " +
                "group by s.symbol " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO2", 4L},
                    new Object[]{"IBM", 6L},
                    new Object[]{"CISCO", 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase4")
    public void aggregationFunctionTestcase5() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase2 - count with different group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, name string, price float, lastClosingPrice float, " +
                        "volume long , quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, name, count() as count " +
                "group by symbol, name " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select s.symbol, s.name, sum(count) as count " +
                "group by s.symbol " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", "CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO2", "WSO2", 4L},
                    new Object[]{"IBM", "IBM", 6L},
                    new Object[]{"CISCO", "CISCO", 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase5")
    public void aggregationFunctionTestcase6() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase2 - count with different group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, name string, price float, lastClosingPrice float, " +
                        "volume long , quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, name, count() as count " +
                "group by symbol, name " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select s.symbol, sum(count) as count " +
                "group by s.symbol " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO21", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO22", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO21", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO22", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM2", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM2", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", "CISCO1", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO2", 4L},
                    new Object[]{"IBM", 6L},
                    new Object[]{"CISCO", 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase6")
    public void aggregationFunctionTestcase7() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase7 - count with different group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, name string, price float, lastClosingPrice float, " +
                        "volume long , quantity int, timestamp long);";
        String query =
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, name, count() as count " +
                "group by symbol, name " +
                "aggregate by timestamp every sec, min ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596434876000L " +
                "per \"seconds\" " +
                "select s.symbol, s.name, sum(count) as count " +
                "group by s.symbol " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO21", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO22", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO21", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", "WSO22", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM2", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM2", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", "IBM1", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", "CISCO1", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO2", "WSO22", 4L},
                    new Object[]{"IBM", "IBM1", 6L},
                    new Object[]{"CISCO", "CISCO1", 1L}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase7")
    public void aggregationFunctionTestcase8() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase8 - count w/o group by on-demand query");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                        "from stockStream " +
                        "select count() as count " +
                        "aggregate by timestamp every sec, min ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);

            Event[] events = siddhiAppRuntime.query("" +
                    "from stockAggregation " +
                    "within 1496200000000L, 1596434876000L " +
                    "per \"seconds\" " +
                    "select AGG_TIMESTAMP, count " +
                    "order by AGG_TIMESTAMP;");

            AssertJUnit.assertNotNull("Event arrived", events);
            AssertJUnit.assertEquals("Number of success events", 7, events.length);


            List<Object[]> actual = new ArrayList<>();
            for (Event event : events) {
                actual.add(event.getData());
            }
            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, 2L},
                    new Object[]{1496289952000L, 2L},
                    new Object[]{1496289954000L, 2L},
                    new Object[]{1496289956000L, 2L},
                    new Object[]{1496290016000L, 1L},
                    new Object[]{1496290076000L, 1L},
                    new Object[]{1496293676000L, 1L}
            );
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isEventsMatch(actual, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase8")
    public void aggregationFunctionTestcase9() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase4 - count with group by on-demand query");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                        "from stockStream " +
                        "select symbol, count() as count " +
                        "group by symbol " +
                        "aggregate by timestamp every sec, min ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");

            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});
            Thread.sleep(2000);


            Event[] events = siddhiAppRuntime.query(
                    "from stockAggregation " +
                            "within 1496200000000L, 1596434876000L " +
                            "per \"seconds\" " +
                            "select symbol, sum(count) as count " +
                            "group by symbol;");

            AssertJUnit.assertNotNull("Event arrived", events);
            AssertJUnit.assertEquals("Number of success events", 3, events.length);


            List<Object[]> actual = new ArrayList<>();
            for (Event event : events) {
                actual.add(event.getData());
            }
            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO2", 4L},
                    new Object[]{"IBM", 6L},
                    new Object[]{"CISCO", 1L}
            );
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(actual, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test(dependsOnMethods = "aggregationFunctionTestcase9")
    public void aggregationFunctionTestcase10() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase9: testing latest incremental aggregator - different select ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as sumPrice " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select i.symbol, sum(s.sumPrice) as sumPrice " +
                "group by s.symbol " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"IBM", 75.0},
                    new Object[]{"IBM", 50.0},
                    new Object[]{"IBM", 130.0},
                    new Object[]{"IBM", 100.0},
                    new Object[]{"IBM", 602.0},
                    new Object[]{"IBM", 1001.0}
            );
            SiddhiTestHelper.waitForEvents(100, 6, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 6, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase10")
    public void aggregationFunctionTestcase11() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase10: testing latest incremental aggregator - different select ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =

                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as sumPrice  " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select s.symbol, sum(s.sumPrice) as sumPrice " +
                "group by i.symbol " +
                "order by AGG_TIMESTAMP " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 1, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    Arrays.equals(inEventsList.get(0), new Object[]{"WSO22", 1958.0}));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "aggregationFunctionTestcase11")
    public void aggregationFunctionTestcase12() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase11: testing latest incremental aggregator - different select ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as sumPrice " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select i.symbol, sum(sumPrice) as totalPrice " +
                "group by i.symbol " +
                "order by AGG_TIMESTAMP " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 1, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    Arrays.equals(inEventsList.get(0), new Object[]{"IBM", 1958.0}));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"aggregationFunctionTestcase12"})
    public void aggregationFunctionTestcase13() throws InterruptedException {
        LOG.info("Use stream name or reference for attributes ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) as " +
                "lastTradeValue, count() as count " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ;" +

                "define stream inputStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "on inputStream.symbol == stockAggregation.symbol " +
                "within \"2017-06-01 04:05:**\" " +
                "per \"seconds\" " +
                "select stockAggregation.symbol, sum(totalPrice) as totalPrice " +
                "group by stockAggregation.symbol " +
                "order by AGG_TIMESTAMP " +
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
                    eventArrived = true;
                }
            });
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:49 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289949000L});
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289949000L});

            // Thursday, June 1, 2017 4:05:48 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289948000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289948000L});

            // Thursday, June 1, 2017 4:05:47 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289947000L});
            stockStreamInputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289947000L});

            // Thursday, June 1, 2017 4:05:46 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496289946000L});

            Thread.sleep(2000);

            inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(100);

            Object[] expected = new Object[]{"IBM", 2000.0};
            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 60000);
            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 1, inEventCount.get());
            AssertJUnit.assertEquals("In events matched", "IBM", inEventsList.get(0)[0]);
            AssertJUnit.assertEquals("In events matched", 2000.0, inEventsList.get(0)[1]);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"aggregationFunctionTestcase13"})
    public void aggregationFunctionTestcase15() throws InterruptedException {
        LOG.info("aggregationFunctionTestcase15 - Group by AGG_TIMESTAMP");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";

        String query =
                "@purge(enable='false')" +
                " define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, sum(price) as totalPrice  " +
                "group by symbol " +
                "aggregate by timestamp every sec...hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        // 1 June 2017 04:05:50
        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler.send(new Object[]{"IBM", 70f, null, 40L, 10, 1496289950000L});

        // 1 June 2017 04:05:52
        stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289952500L});

        // 1 June 2017 04:05:54
        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
        stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 96, 1496289954500L});

        Thread.sleep(2000);

        Event[] events = siddhiAppRuntime.query(
                "from stockAggregation " +
                        "within \"2017-06-** **:**:**\" " +
                        "per \"seconds\" " +
                        "select AGG_TIMESTAMP, sum(totalPrice) as totalPrice " +
                        "group by AGG_TIMESTAMP;");
        EventPrinter.print(events);

        Assert.assertNotNull(events);
        AssertJUnit.assertEquals(3, events.length);

        List<Object[]> eventsOutputList = new ArrayList<>();
        for (Event event : events) {
            eventsOutputList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1496289950000L, 120.0},
                new Object[]{1496289952000L, 160.0},
                new Object[]{1496289954000L, 200.0}
        );
        AssertJUnit.assertTrue("In events matched",
                SiddhiTestHelper.isUnsortedEventsMatch(eventsOutputList, expected));

        siddhiAppRuntime.shutdown();
    }

}

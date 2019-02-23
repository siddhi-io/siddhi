/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LatestAggregationTestCase {

    private static final Logger LOG = Logger.getLogger(LatestAggregationTestCase.class);
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
    public void latestTestCase() throws InterruptedException {
        LOG.info("latestTestCase: testing latest incremental aggregator");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "define aggregation stockAggregation " +
                        "from stockStream " +
                        "select symbol, avg(price) as avgPrice, (price * quantity) as latestPrice " +
                        "aggregate by timestamp every sec...year ;" +

                        "define stream inputStream (symbol string); " +

                        "@info(name = 'query1') " +
                        "from inputStream as i join stockAggregation as s " +
                        "within 1496200000000L, 1596535449000L " +
                        "per \"seconds\" " +
                        "select AGG_TIMESTAMP, s.symbol, s.latestPrice " +
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

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});
            Thread.sleep(10000);
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
                    new Object[]{1496289950000L, "WSO22", 750f},
                    new Object[]{1496289952000L, "WSO24", 1600f},
                    new Object[]{1496289954000L, "IBM1", 10200f},
                    new Object[]{1496289956000L, "IBM1", 3500f}
            );
            SiddhiTestHelper.waitForEvents(100, 4, inEventCount, 10000);
            AssertJUnit.assertEquals("In events matched", true,
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
            AssertJUnit.assertEquals("Remove events matched", true,
                    SiddhiTestHelper.isUnsortedEventsMatch(removeEventsList, expected));
            AssertJUnit.assertEquals("Number of success events", 4, inEventCount.get());
            AssertJUnit.assertEquals("Number of remove events", 4, removeEventCount.get());
            AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

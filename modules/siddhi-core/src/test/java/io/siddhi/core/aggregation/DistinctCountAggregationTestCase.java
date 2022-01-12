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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DistinctCountAggregationTestCase {

    private static final Logger LOG = LogManager.getLogger(DistinctCountAggregationTestCase.class);
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
    public void incrementalStreamProcessorTest1() throws InterruptedException {
        LOG.info("incrementalStreamProcessorTest1: testing distinctCount incremental aggregator");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "define aggregation stockAggregation " +
                        "from stockStream " +
                        "select distinctCount(symbol) as distinctCnt " +
                        "aggregate by timestamp every sec...year ;" +

                        "define stream inputStream (symbol string); " +

                        "@info(name = 'query1') " +
                        "from inputStream as i join stockAggregation as s " +
                        "within 1496200000000L, 1596535449000L " +
                        "per \"days\" " +
                        "select AGG_TIMESTAMP, s.distinctCnt " +
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
            stockStreamInputHandler.send(new Object[]{"WSO22", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM2", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});

            // Thursday, June 1, 2017 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 61f, 44f, 200L, 56, 1496297276000L});

            // Friday, June 2, 2017 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 801f, null, 100L, 10, 1496383676000L});

            // Saturday, June 3, 2017 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 901f, null, 100L, 15, 1496470076000L});

            // Monday, July 3, 2017 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 96, 1499062076000L});

            // Thursday, August 3, 2017 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 402f, null, 200L, 9, 1501740476000L});

            // Friday, August 3, 2018 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 63f, 44f, 200L, 6, 1533276476000L});

            // Saturday, August 3, 2019 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 260f, 44f, 200L, 16, 1564812476000L});

            // Monday, August 3, 2020 6:07:56 AM
            stockStreamInputHandler.send(new Object[]{"CISCO", 26f, 44f, 200L, 16, 1596434876000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496275200000L, 8},
                    new Object[]{1496361600000L, 1},
                    new Object[]{1496448000000L, 1},
                    new Object[]{1499040000000L, 1},
                    new Object[]{1501718400000L, 1},
                    new Object[]{1533254400000L, 1},
                    new Object[]{1564790400000L, 1},
                    new Object[]{1596412800000L, 1}
            );
            SiddhiTestHelper.waitForEvents(100, 8, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 8, inEventCount.get());
            AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isEventsMatch(inEventsList, expected));

            AssertJUnit.assertEquals("Number of remove events", 8, removeEventCount.get());
            AssertJUnit.assertTrue("Remove events matched",
                    SiddhiTestHelper.isEventsMatch(removeEventsList, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

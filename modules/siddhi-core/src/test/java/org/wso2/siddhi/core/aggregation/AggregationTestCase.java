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

package org.wso2.siddhi.core.aggregation;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

public class AggregationTestCase {

    static final Logger LOG = Logger.getLogger(AggregationTestCase.class);
    private AtomicInteger inEventCount;
    private AtomicInteger removeEventCount;
    private boolean eventArrived;
    @BeforeMethod
    public void init() {
        inEventCount = new AtomicInteger(0);
        removeEventCount = new AtomicInteger(0);
        eventArrived = false;
    }

    @Test
    public void incrementalStreamProcessorTest1() {
        LOG.info("Incremental Processing: incrementalStreamProcessorTest1");
        // Test Aggregation parsing: Without group by, external time based,
        // time range (all time durations from sec to min)
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                " define stream cseEventStream (arrival long, symbol string, price float, volume int); ";

        String query = "" +
                " @info(name = 'query1') " +
                " define aggregation cseEventAggregation " +
                " from cseEventStream " +
                " select sum(price) as sumPrice " +
                " aggregate by arrival every sec ... min";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void incrementalStreamProcessorTest2() {
        LOG.info("Incremental Processing: incrementalStreamProcessorTest2");
        // Test Aggregation parsing: Without group by, event time based,
        // time range (all time durations from sec to min)
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                " define stream cseEventStream (arrival long, symbol string, price float, volume int); ";

        String query = "" +
                " @info(name = 'query2') " +
                " define aggregation cseEventAggregation " +
                " from cseEventStream " +
                " select sum(price) as sumPrice " +
                " aggregate every sec ... min";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void incrementalStreamProcessorTest3() {
        LOG.info("Incremental Processing: incrementalStreamProcessorTest3");
        // Test Aggregation parsing: With group by (one attribute),
        // event time based, comma separated durations
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                " define stream cseEventStream (arrival long, symbol string, price float, volume int); ";

        String query = "" +
                " @info(name = 'query3') " +
                " define aggregation cseEventAggregation " +
                " from cseEventStream " +
                " select sum(price) as sumPrice " +
                " group by price " +
                " aggregate every sec, min, hour, day";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void incrementalStreamProcessorTest4() {
        LOG.info("Incremental Processing: incrementalStreamProcessorTest4");
        // Test Aggregation parsing: With group by (two attributes), event time based, comma separated durations
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                " define stream cseEventStream (arrival long, symbol string, price float, volume int); ";

        String query = "" +
                " @info(name = 'query3') " +
                " define aggregation cseEventAggregation " +
                " from cseEventStream " +
                " select sum(price) as sumPrice " +
                " group by price, volume " +
                " aggregate every sec, min, hour, day";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void externalTimeTest1() throws InterruptedException {
        LOG.info("Incremental Processing: externalTimeTest1");
        // Without group by. Buffer size = 0.
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price1 float, " +
                "                              price2 float, volume long , quantity int, timestamp long);";
        String query = "" +
                "define aggregation test " +
                "from cseEventStream " +
                "select symbol as s, avg(price1) as avgPrice, sum(price1) as totprice1, count() as countPrice, " +
                "(quantity * volume) as mult  " +
                "aggregate by timestamp every sec...year ;" +

                "define stream barStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +
                "" +
                "@info(name = 'query1') " +
                "from barStream as b join test as a " +
                "within 1496200000000L, 1596434876000L " +
                "per \"days\" " +
                "select a.avgPrice, a.totprice1 as sumPrice, a.mult, a.countPrice  " +
                "insert all events into fooBar; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler barStreamInputHandler = siddhiAppRuntime.getInputHandler("barStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            inputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            inputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            inputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            inputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});

            // Thursday, June 1, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496297276000L});

            // Friday, June 2, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 800f, null, 100L, 10, 1496383676000L});

            // Saturday, June 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 900f, null, 100L, 15, 1496470076000L});

            // Monday, July 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1499062076000L});

            // Thursday, August 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1501740476000L});

            // Friday, August 3, 2018 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 6, 1533276476000L});

            // Saturday, August 3, 2019 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 260f, 44f, 200L, 16, 1564812476000L});

            // Monday, August 3, 2020 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 260f, 44f, 200L, 16, 1596434876000L});

            Thread.sleep(2000);

            barStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(50);
        }  finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void externalTimeTest() throws InterruptedException {
        LOG.info("Incremental Processing: externalTimeTest");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price1 float, " +
                "                              price2 float, volume long , quantity int, timestamp long); ";
        String query = "" +
                "define aggregation test " +
                "from cseEventStream " +
                "select symbol, avg(price1) as avgPrice, sum(price1) as totprice1, (quantity * volume) as mult  " +
                "group by symbol " +
                "aggregate by timestamp every sec...year; " +

                "define stream barStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +
                "" +
                "@info(name = 'query1') " +
                "from barStream as b join test as a " +
                "within \"2017-06-** **:**:**\" " +
                "per \"seconds\" " +
                "select a.symbol, a.avgPrice, a.totprice1 as sumPrice, a.mult  " +
                "insert all events into fooBar; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler barStreamInputHandler = siddhiAppRuntime.getInputHandler("barStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            inputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:54 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            inputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            inputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            inputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 700f, null, 200L, 20, 1496293676000L});

            // Thursday, June 1, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496297276000L});

            // Friday, June 2, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 800f, null, 100L, 10, 1496383676000L});

            // Saturday, June 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 900f, null, 100L, 15, 1496470076000L});

            // Monday, July 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1499062076000L});

            // Thursday, August 3, 2017 6:07:56 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1501740476000L});

            // Friday, August 3, 2018 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 6, 1533276476000L});

            // Saturday, August 3, 2019 6:07:56 AM
            inputHandler.send(new Object[]{"WSO2", 260f, 44f, 200L, 16, 1564812476000L});

            // Monday, August 3, 2020 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 260f, 44f, 200L, 16, 1596434876000L});

            // Monday, December 3, 2020 6:07:56 AM
            inputHandler.send(new Object[]{"CISCO", 260f, 44f, 200L, 16, 1606975676000L});

            Thread.sleep(2000);
            barStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(50);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void eventTimeTest() throws InterruptedException {
        LOG.info("Incremental Processing: eventTimeTest");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price1 float, " +
                "                              price2 float, volume long , quantity int, timestamp long);";
        String query = " define aggregation test " +
                "from cseEventStream " +
                "select symbol, avg(price1) as avgPrice, sum(price1) as totprice1, (quantity * volume) as mult  " +
                "group by symbol " +
                "aggregate every sec...hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});
        Thread.sleep(2000);

        inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
        inputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});
        Thread.sleep(2000);

        inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
        inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});
        Thread.sleep(2000);

//        inputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
//        inputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});
//        Thread.sleep(60000);
//
//        inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});
//        Thread.sleep(60000);
        Event[] events = siddhiAppRuntime.query("from test " +
                "on symbol == \"IBM\" " +
                "within \"2017-08-** **:**:** +05:30\" " +
                "per \"seconds\"");
        EventPrinter.print(events);
        Thread.sleep(50);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void externalTimeOutOfOrderTest1() throws InterruptedException {
        LOG.info("Incremental Processing: externalTimeOutOfOrderTest1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price1 float, " +
                "                              price2 float, volume long , quantity int, timestamp long);";
        String query = "" +
                "@BufferSize('3') " +
                "define aggregation test " +
                "from cseEventStream " +
                "select symbol, avg(price1) as avgPrice, sum(price1) as totprice1, (quantity * volume) as mult  " +
                "group by symbol " +
                "aggregate by timestamp every sec...year ; " +

                "define stream barStream (sym string, value int, startTime string, " +
                "endTime string, perValue string); " +
                "" +
                "@info(name = 'query1') " +
                "from barStream join test " +
                "on sym == symbol " +
                "within \"2017-06-01 09:35:50 +05:30\", \"2017-06-01 10:37:57 +05:30\" " +
                "per \"seconds\" " +
                "select sym, avgPrice, totprice1 as sumPrice, mult  " +
                "insert all events into fooBar; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler barStreamInputHandler = siddhiAppRuntime.getInputHandler("barStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM (add 5.30 to get corresponding IST time)
            inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});

            // Thursday, June 1, 2017 4:05:52 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
            inputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM (out of order. must be processed with 1st event)
            inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:54 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});

            // Thursday, June 1, 2017 4:05:50 AM (out of order. should be processed for 1st second.
            // However, since 1st second's data has already been sent to next executor, this would
            // be processed with current sec data.
            inputHandler.send(new Object[]{"IBM", 50f, 60f, 90L, 6, 1496289950000L});

            // Thursday, June 1, 2017 4:05:56 AM
            inputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            inputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289956000L});

            // Thursday, June 1, 2017 4:06:56 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496290016000L});

            // Thursday, June 1, 2017 4:07:56 AM
            inputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496290076000L});

            // Thursday, June 1, 2017 5:07:56 AM
            inputHandler.send(new Object[]{"IBM", 700f, null, 200L, 20, 1496293676000L});

            Thread.sleep(2000);
            barStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(50);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void externalTimeOutOfOrderTest2() throws InterruptedException {
        LOG.info("Incremental Processing: externalTimeOutOfOrderTest2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price1 float, " +
                "                              price2 float, volume long , quantity int, timestamp long);";
        String query = "" +
                "@BufferSize('3') " +
                "define aggregation test " +
                "from cseEventStream " +
                "select symbol, avg(price1) as avgPrice, sum(price1) as totprice1, (quantity * volume) as mult  " +
                "group by symbol " +
                "aggregate by timestamp every sec...year  ; " +

                "define stream barStream (sym string, value int, startTime string, " +
                "endTime string, perValue string); " +
                "" +
                "@info(name = 'query1') " +
                "from barStream join test " +
                "within \"2017-06-01 04:05:50\", \"2017-06-01 04:06:57\" " +
                "per \"seconds\" " +
                "select symbol, avgPrice, totprice1 as sumPrice, mult  " +
                "insert all events into fooBar; ";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler barStreamInputHandler = siddhiAppRuntime.getInputHandler("barStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:53 AM
            inputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289953000L});
            inputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289953000L});

            // Thursday, June 1, 2017 4:05:52 AM
            inputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289952000L});
            inputHandler.send(new Object[]{"IBM", 500f, null, 200L, 7, 1496289952000L});

            // Thursday, June 1, 2017 4:05:51 AM
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289951000L});
            inputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289951000L});

            // Thursday, June 1, 2017 4:05:53 AM
            inputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496289953000L});

            // Thursday, June 1, 2017 4:05:50 AM
            inputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});

            // Thursday, June 1, 2017 4:05:54 AM
            inputHandler.send(new Object[]{"IBM", 600f, null, 200L, 6, 1496289954000L});

            // Thursday, June 1, 2017 4:06:56 AM
            inputHandler.send(new Object[]{"IBM", 1000f, null, 200L, 9, 1496290016000L});

            Thread.sleep(2000);
            barStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            Thread.sleep(50);
        }   finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void incrementalAggregationRetrievalTest1() throws InterruptedException {
        LOG.info("Incremental Processing: incrementalAggregationRetrievalTest1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream cseEventStream (arrival long, symbol string, price float, " +
                "volume int, timeStamp string); " +
                " " +
                "@BufferSize('3') " +
                "define aggregation cseEventAggregation " +
                "from cseEventStream " +
                "select symbol as s, sum(price) as total, avg(price) as avgPrice, count() as countPrice, " +
                "max(volume) as maxVol, min(volume) as minVol " +
                "group by symbol " +
                "aggregate by timeStamp every sec ... min; " +

                "define stream barStream (symbol string, value int, startTime string, " +
                "endTime string, perValue string); " +
                "" +
                "@info(name = 'query1') " +
                "from barStream as b join cseEventAggregation as a " +
                "on a.s == b.symbol " +
                "within \"2017-06-** **:**:** +05:30\" " +
                "per \"minutes\" " +
                "select a.s, a.avgPrice, a.total as sumPrice, a.minVol, a.maxVol, b.value, a.countPrice " +
                "insert all events into fooBar; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount.addAndGet(inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler cseEventStreamInputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler barStreamInputHandler = siddhiAppRuntime.getInputHandler("barStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM (add 5.30 to get corresponding IST time)
            cseEventStreamInputHandler.send(new Object[]{1496289950000L, "WSO2", 50f, 6, "2017-06-01 04:05:50"});
            cseEventStreamInputHandler.send(new Object[]{1496289950000L, "WSO2", 70f, 10, "2017-06-01 04:05:50"});

            // Thursday, June 1, 2017 4:05:51 AM
            cseEventStreamInputHandler.send(new Object[]{1496289951000L, "IBM", 100f, 26, "2017-06-01 04:05:51"});
            cseEventStreamInputHandler.send(new Object[]{1496289951000L, "IBM", 100f, 96, "2017-06-01 04:05:51"});

            // Thursday, June 1, 2017 4:05:52 AM
            cseEventStreamInputHandler.send(new Object[]{1496289952000L, "IBM", 900f, 60, "2017-06-01 04:05:52"});
            cseEventStreamInputHandler.send(new Object[]{1496289952000L, "IBM", 500f, 7, "2017-06-01 04:05:52"});

            // Thursday, June 1, 2017 4:05:53 AM
            cseEventStreamInputHandler.send(new Object[]{1496289953000L, "WSO2", 60f, 56, "2017-06-01 04:05:53"});
            cseEventStreamInputHandler.send(new Object[]{1496289953000L, "WSO2", 100f, 16, "2017-06-01 04:05:53"});
            cseEventStreamInputHandler.send(new Object[]{1496289953000L, "IBM", 400f, 9, "2017-06-01 04:05:53"});

            // Thursday, June 1, 2017 4:05:54 AM
            cseEventStreamInputHandler.send(new Object[]{1496289954000L, "IBM", 600f, 6, "2017-06-01 04:05:54"});

            Thread.sleep(500);

            barStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:51 +05:30",
                    "2017-06-01 09:35:52 +05:30", "seconds"});
            SiddhiTestHelper.waitForEvents(100, 1, inEventCount, 6000);
            SiddhiTestHelper.waitForEvents(100, 1, removeEventCount, 6000);
            AssertJUnit.assertEquals(1, inEventCount.get());
            AssertJUnit.assertEquals(1, removeEventCount.get());
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

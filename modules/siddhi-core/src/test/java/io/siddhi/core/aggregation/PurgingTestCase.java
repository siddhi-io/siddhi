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
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class PurgingTestCase {
    private static final Logger LOG = LogManager.getLogger(PurgingTestCase.class);

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void incrementalPurgingTest1() {
        LOG.info("incrementalPurgingTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream = " define stream stockStream (arrival long, symbol string, price float, volume int); ";
        String query = " @info(name = 'query1') " +
                " @purge(enable='true',interval='1 min',@retentionPeriod(sec='120 sec',min='2 h',hours='25 h'))" +
                " define aggregation stockAggregation " +
                " from stockStream " +
                " select sum(price) as sumPrice " +
                " aggregate by arrival every sec...min";
        siddhiManager.createSiddhiAppRuntime(stockStream + query);
    }

    @Test
    public void incrementalPurgingTest2() throws InterruptedException {
        LOG.info("incrementalPurgingTest2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream = " define stream stockStream (symbol string, price float, lastClosingPrice float," +
                " volume long ,quantity int);";
        String query = "  @purge(enable='true',interval='5 sec',@retentionPeriod(sec='120 sec',min='all',hours='all'" +
                "                ,days='all',months='all',years='all'))  " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "group by symbol " +
                "aggregate every sec...years ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);
        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        Thread.sleep(1000);
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        Thread.sleep(1000);
        stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56});
        Thread.sleep(1000);
        stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16});
        Thread.sleep(1000);
        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26});
        Thread.sleep(1000);
        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"" + Calendar.getInstance().get(Calendar.YEAR) + "-**-** **:**:**\" " +
                "per \"seconds\"");
        EventPrinter.print(events);

        AssertJUnit.assertNotNull("Initial queried events cannot be null", events);
        AssertJUnit.assertEquals("Number of events before purging", 6, events.length);
        Thread.sleep(120100);

        events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"" + Calendar.getInstance().get(Calendar.YEAR) + "-**-** **:**:**\" " +
                "per \"seconds\"");
        EventPrinter.print(events);
        AssertJUnit.assertNull("No events expected after purging", events);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void incrementalPurgingTestCase3() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream = "define stream stockStream (symbol string, price float, lastClosingPrice float, " +
                "volume long , quantity int, timestamp long); ";
        String query = " @purge(enable='true',interval='10 sec',@retentionPeriod(sec='120 sec',min='all',hours='all'," +
                "days='all',months='all',years='all'))   " +
                "define aggregation stockAggregation " +
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
            Thread.sleep(1000);
            // Thursday, June 1, 2017 4:05:51 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
            Thread.sleep(1000);
            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 400f, null, 200L, 9, 1496289952000L});
            Thread.sleep(1000);
            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289950000L});
            Thread.sleep(1000);
            stockStreamInputHandler.send(new Object[]{"WSO2", 500f, null, 200L, 7, 1496289951011L});
            Thread.sleep(1000);
            // Thursday, June 1, 2017 4:05:53 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289953000L});
            Thread.sleep(1000);
            stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 96, 1496289953000L});
            Thread.sleep(2000);

            Event[] events = siddhiAppRuntime.query("from stockAggregation within 0L, 1543664151000L per " +
                    "'seconds' select AGG_TIMESTAMP, symbol, totalPrice ");
            EventPrinter.print(events);

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
            AssertJUnit.assertTrue("Data Matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected));
            Thread.sleep(120000);

            events = siddhiAppRuntime.query("from stockAggregation within 0L, 1543664151000L per " +
                    "'seconds' select AGG_TIMESTAMP, symbol, totalPrice ");
            EventPrinter.print(events);
            Assert.assertNull(events);

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

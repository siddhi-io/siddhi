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
import io.siddhi.core.exception.OnDemandQueryCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OnDemandQueryWindowTestCase {

    private static final Logger log = LogManager.getLogger(OnDemandQueryWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void test1() throws InterruptedException {
        log.info("Test1 window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define window StockWindow (symbol string, price float, volume long) length(2); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockWindow ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockWindow ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > 75 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > volume*3/4  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test
    public void test2() throws InterruptedException {
        log.info("Test2 window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define window StockWindow (symbol string, price float, volume long) length(3); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockWindow ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > 75 " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);

        events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > 5 " +
                "select symbol, volume " +
                "having symbol == 'WSO2' ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        siddhiAppRuntime.shutdown();

    }

    @Test
    public void test3() throws InterruptedException {
        log.info("Test3 window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define window StockWindow (symbol string, price float, volume long) length(3); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockWindow ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > 5 " +
                "select symbol, sum(volume) as totalVolume " +
                "group by symbol " +
                "having totalVolume >150 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(200L, events[0].getData(1));

        events = siddhiAppRuntime.query("" +
                "from StockWindow " +
                "on price > 5 " +
                "select symbol, sum(volume) as totalVolume " +
                "group by symbol  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = OnDemandQueryCreationException.class)
    public void test5() throws InterruptedException {
        log.info("Test5 table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define window StockWindow (symbol string, price float, volume long) length(3); ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        try {

            siddhiAppRuntime.start();

            Event[] events = siddhiAppRuntime.query("" +
                    "from StockWindow1 " +
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
}

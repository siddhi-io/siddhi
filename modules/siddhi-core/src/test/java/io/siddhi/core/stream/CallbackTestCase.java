/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.stream;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.DefinitionNotExistException;
import io.siddhi.core.exception.QueryNotExistException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * Created on 1/24/15.
 */
public class CallbackTestCase {

    private static final Logger log = LogManager.getLogger(CallbackTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private volatile boolean eventArrived2;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        eventArrived2 = false;
    }

    @Test
    public void callbackTest1() throws InterruptedException {
        log.info("callback test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('callbackTest1') " +
                "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[70 > price] " +
                "select symbol, price " +
                "insert into outputStream;";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count = count + inEvents.length;
                eventArrived = true;
            }

        });

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                count = count + events.length;
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        Thread.sleep(100);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = QueryNotExistException.class)
    public void callbackTest2() throws InterruptedException {
        log.info("callback test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('callbackTest1') " +
                "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[70 > price] " +
                "select symbol, price " +
                "insert into outputStream;";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
            }

        });

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = DefinitionNotExistException.class)
    public void callbackTest3() throws InterruptedException {
        log.info("callback test3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('callbackTest1') " +
                "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[70 > price] " +
                "select symbol, price " +
                "insert into outputStream;";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void callbackTest4() throws InterruptedException {
        log.info("callback test4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('callbackTest1') " +
                "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[70 > price] " +
                "select symbol, price " +
                "insert into OutputStream;";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("StockStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                Map<String, Object>[] eventMap = toMap(events);
                Assert.assertEquals(eventMap.length, events.length);
                Assert.assertEquals(eventMap[0].get("_timestamp"), events[0].getTimestamp());
                Assert.assertEquals(eventMap[1].get("_timestamp"), events[1].getTimestamp());
                Assert.assertEquals(eventMap[1].get("volume"), events[1].getData(2));
                count = count + events.length;
                eventArrived = true;
            }
        });

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                Map<String, Object>[] eventMap = toMap(events);
                Assert.assertEquals(eventMap.length, events.length);
                Assert.assertEquals(eventMap[0].get("_timestamp"), events[0].getTimestamp());
                Assert.assertEquals(eventMap[0].get("price"), events[0].getData(1));
                Assert.assertEquals(eventMap[0].get("symbol"), events[0].getData(0));
                Assert.assertNull(eventMap[0].get("volume"));
                count = count + events.length;
                eventArrived2 = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        inputHandler.send(new Event[]{
                new Event(System.currentTimeMillis(), new Object[]{"IBM", 700f, 100L}),
                new Event(System.currentTimeMillis(), new Object[]{"WSO2", 60.5f, 200L})});
        Thread.sleep(100);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(eventArrived2);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void callbackTest5() throws InterruptedException {
        log.info("callback test5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price , symbol as sym1 " +
                "insert into OutputStream ;";

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                if (events != null) {
                    count += events.length;
                }
            }
        };
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("OutputStream", streamCallback);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 0f, 100L});
        siddhiAppRuntime.removeCallback(streamCallback);
        inputHandler.send(new Object[]{"WSO2", 0f, 100L});
        SiddhiTestHelper.waitForEvents(10, count, 1, 100);
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

}

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
package io.siddhi.core.query;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
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

import java.util.concurrent.atomic.AtomicBoolean;

public class OrderByLimitTestCase {
    private static final Logger log = LogManager.getLogger(OrderByLimitTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private AtomicBoolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = new AtomicBoolean(false);
    }

    @Test
    public void limitTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, price, volume " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(0) || inEvents[0].getData(2).equals(4));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, price, volume " +
                "order by symbol " +
                "limit 3 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(3, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(2) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"AAA", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"IBM", 601.5f, 6});
        inputHandler.send(new Object[]{"BBB", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(6, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(3) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "order by symbol " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(3) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest5() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(volume) as totalVolume, volume, price " +
                "group by symbol " +
                "order by price, totalVolume " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(0) || inEvents[0].getData(2).equals(4));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 60.5f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 60.5f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest6() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "group by symbol " +
                "order by totalPrice " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(3) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest7() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, price, volume " +
                "group by symbol " +
                "order by price " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(1) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void limitTest9() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "group by symbol " +
                "order by price " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest10() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "group by symbol " +
                "order by totalPrice desc " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(2) || inEvents[0].getData(2).equals(4));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 7060.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest11() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest12() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "group by symbol " +
                "order by totalPrice desc " +
                "offset 1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(1) || inEvents[0].getData(2).equals(3)
                        || inEvents[0].getData(2).equals(6) || inEvents[0].getData(2).equals(7));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 7060.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest13() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "offset 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest14() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, sum(price) as totalPrice, volume " +
                "group by symbol " +
                "order by totalPrice desc " +
                "limit 1 " +
                "offset 1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                Assert.assertTrue(inEvents[0].getData(2).equals(1) || inEvents[0].getData(2).equals(6));
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 7060.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest15() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit 2 " +
                "offset 2 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(2, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest16() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit 1 " +
                "offset 1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(0, inEventCount);
        AssertJUnit.assertFalse(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void limitTest17() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit 1 " +
                "offset 0 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"IBM", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"XYZ", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 7});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 500);
        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = {SiddhiAppCreationException.class})
    public void limitTest18() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit -1 " +
                "offset 0 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });
        try {
            siddhiAppRuntime.start();
        } finally {
            siddhiAppRuntime.shutdown();

        }
    }

    @Test(expectedExceptions = {SiddhiAppCreationException.class})
    public void limitTest19() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(4) " +
                "select symbol, price, volume " +
                "order by price asc " +
                "limit 1 " +
                "offset -1 " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals(1, inEvents.length);
                inEventCount = inEventCount + inEvents.length;
                eventArrived.set(true);
            }

        });
        try {
            siddhiAppRuntime.start();
        } finally {
            siddhiAppRuntime.shutdown();

        }
    }
}

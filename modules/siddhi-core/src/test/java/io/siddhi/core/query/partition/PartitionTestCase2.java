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
package io.siddhi.core.query.partition;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
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

import java.util.concurrent.atomic.AtomicInteger;

public class PartitionTestCase2 {

    private static final Logger log = LogManager.getLogger(PartitionTestCase2.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int stockStreamEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {

        count.set(0);
        eventArrived = false;
        stockStreamEventCount = 0;
    }

    @Test
    public void testModExpressionExecutorFloatCase() throws InterruptedException {

        log.info("Partition testModExpressionExecutorFloatCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testModExpressionExecutorFloatCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 float, " +
                "atr5 long,  atr6 long,  atr7 float,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4%atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(9.559998f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(9.74f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(11.540001f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(13.660004f, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0f, 500L, 200L, 11.43f, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0f, 501L, 201L, 15.21f, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0f, 502L, 202L, 45.23f, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0f, 502L, 202L, 87.34f, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testModExpressionExecutorLongCase() throws InterruptedException {

        log.info("Partition testModExpressionExecutorLongCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testModExpressionExecutorLongCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr5%atr6 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(0L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(89L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(98L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(84L, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testModExpressionExecutorIntCase() throws InterruptedException {

        log.info("Partition testModExpressionExecutorIntCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testModExpressionExecutorIntCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr3%atr11 as dividedVal, atr5 as threshold,  atr1 " +
                "as symbol," + "cast(atr2, 'double') as priceInDouble, sum(atr7) as summedValue insert" +
                " into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(8, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(35, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(4, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(32, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 23});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 65});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 12});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 34});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorDoubleCase() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorDoubleCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorDoubleCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4-atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(89.57, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(85.78999999999999, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(56.77, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(13.659999999999997, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 200L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 201L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 202L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorFloatCase() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorFloatCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorFloatCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 float, " +
                "atr5 long,  atr6 long,  atr7 float,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4-atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(89.57f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(85.79f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(56.77f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(13.660004f, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0f, 500L, 200L, 11.43f, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0f, 501L, 201L, 15.21f, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0f, 502L, 202L, 45.23f, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0f, 502L, 202L, 87.34f, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorLongCase() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorLongCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorLongCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr5-atr6 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(480L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(295L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(300L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(293L, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorIntCase() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorIntCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorIntCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr3-atr11 as dividedVal, atr5 as threshold,  atr1 " +
                "as symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert" +
                " into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(77, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(35, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(88, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(66, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 23});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 65});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 12});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 34});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorDoubleCase() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorDoubleCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorDoubleCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4*atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(1154.43, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(1536.21, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(4613.46, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(8821.34, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 200L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 201L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 202L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorFloatCase() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorFloatCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorFloatCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 float, " +
                "atr5 long,  atr6 long,  atr7 float,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4*atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(1154.43f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(1536.21f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(4613.46f, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(8821.34f, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0f, 500L, 200L, 11.43f, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0f, 501L, 201L, 15.21f, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0f, 502L, 202L, 45.23f, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0f, 502L, 202L, 87.34f, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorLongCase() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorLongCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorLongCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr5*atr6 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(10000L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(103206L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(101404L, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(104918L, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorIntCase() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorIntCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorIntCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr3*atr11 as dividedVal, atr5 as threshold,  atr1 " +
                "as symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert" +
                " into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(2300, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(6500, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(1200, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(3400, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, 100, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 23});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 65});
        inputHandler.send(new Object[]{"IBM", null, 100, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 12});
        inputHandler.send(new Object[]{"ORACLE", null, 100, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 34});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void partitionStreamValidationTest() throws InterruptedException {

        log.info("filter test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@App:name(\"ExtremaBottomKLeng\")\n" +
                "@App:Description('Demonstrates how to use the siddhi-execution-extrema with " +
                "bottomKLengthBatch function')\n" +
                "\n" +
                "define stream inputStream (item string, price long);\n" +
                "\n" +
                "@sink(type='log') \n" +
                "define stream outputStream(item string, price long);\n" +
                "\n" +
                "partition with (itemsss of inputStreamssss)\n" +
                "begin \n" +
                "    from inputStream select item\n" +
                "    insert into s\n" +
                "end;\n" +

                "from inputStream\n" +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        try {
            log.info("Running : " + siddhiAppRuntime.getName());

            siddhiAppRuntime.start();
        } finally {
            siddhiAppRuntime.shutdown();

        }
    }

    @Test
    public void testPartitionQuery() throws InterruptedException {

        log.info("Partition test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('PartitionTest') " +
                "define stream streamA (ts long, symbol string, price int);" +
                "partition with (symbol of streamA) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from streamA#window.lengthBatch(2, true) " +
                "   select symbol, sum(price) as total " +
                "   insert all events into StockQuote ;  " +
                "end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                AssertJUnit.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                int eventCount = count.addAndGet(events.length);
                eventArrived = true;
                if (eventCount == 1) {
                    Assert.assertEquals(events[0].getData(1), 700L);
                } else if (eventCount == 2) {
                    Assert.assertEquals(events[0].getData(1), 60L);
                } else if (eventCount == 3) {
                    Assert.assertEquals(events[0].getData(1), 120L);
                } else if (eventCount == 4) {
                    Assert.assertEquals(events[0].getData(1), 60L);
                } else if (eventCount == 5) {
                    Assert.assertEquals(events[0].getData(1), 1400L);
                } else if (eventCount == 6) {
                    Assert.assertEquals(events[0].getData(1), 700L);
                }
            }
        };
        siddhiAppRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("streamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{100L, "IBM", 700});
        inputHandler.send(new Object[]{101L, "WSO2", 60});
        inputHandler.send(new Object[]{101L, "WSO2", 60});
        inputHandler.send(new Object[]{1134L, "WSO2", 60});
        inputHandler.send(new Object[]{100L, "IBM", 700});
        inputHandler.send(new Object[]{1145L, "IBM", 700});
        SiddhiTestHelper.waitForEvents(100, 6, count, 60000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(6, count.get());

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorDoubleCase1() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorDoubleCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorDoubleCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4*atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "bb", 100, null, 500L, 200L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, null, 501L, 201L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", "cc", 100, 102.0, 502L, 202L, null, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", "dd", 100, null, 502L, 202L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorFloatCase1() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorFloatCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorFloatCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 float, " +
                "atr5 long,  atr6 long,  atr7 float,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4*atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "bb", 100, null, 500L, 200L, 11.43f, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, null, 501L, 201L, 15.21f, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", "cc", 100, 102.0f, 502L, 202L, null, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", "dd", 100, null, 502L, 202L, 87.34f, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMultiplyExpressionExecutorIntCase1() throws InterruptedException {

        log.info("Partition testMultiplyExpressionExecutorIntCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testMultiplyExpressionExecutorIntCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr3*atr11 as dividedVal, atr5 as threshold,  atr1 " +
                "as symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert" +
                " into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "aa", null, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 23});
        inputHandler.send(new Object[]{"WSO2", "aa", null, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 65});
        inputHandler.send(new Object[]{"IBM", "cc", null, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 12});
        inputHandler.send(new Object[]{"ORACLE", "dd", null, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 34});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorDoubleCase1() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorDoubleCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorDoubleCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4-atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "bb", 100, null, 500L, 200L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, null, 501L, 201L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", "cc", 100, null, 502L, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", "dd", 100, null, 502L, 202L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorFloatCase1() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorFloatCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorFloatCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 float, " +
                "atr5 long,  atr6 long,  atr7 float,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr4-atr7 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "aa", 100, null, 500L, 200L, 11.43f, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0f, 501L, 201L, null, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", "nn", 100, 102.0f, 502L, 202L, null, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", "mm", 100, null, 502L, 202L, 87.34f, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorLongCase1() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorLongCase1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorLongCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr5-atr6 as dividedVal, atr5 as threshold,  atr1 as" +
                " symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert " +
                "into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "mm", 100, 101.0, null, 20L, 11.43, 75.7f, false, true, 105});
        inputHandler.send(new Object[]{"WSO2", "aa", 100, 101.0, null, 206L, 15.21, 76.7f, false, true, 106});
        inputHandler.send(new Object[]{"IBM", "nj", 100, 102.0, null, 202L, 45.23, 77.7f, false, true, 107});
        inputHandler.send(new Object[]{"ORACLE", "gg", 100, 101.0, null, 209L, 87.34, 77.7f, false, false, 108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(0, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testSubtractExpressionExecutorIntCase1() throws InterruptedException {

        log.info("Partition testSubtractExpressionExecutorIntCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "@app:name('testSubtractExpressionExecutorIntCase') " +
                "define stream cseEventStream (atr1 string,  atr2 string, atr3 int, atr4 double, " +
                "atr5 long,  atr6 long,  atr7 double,  atr8 float , atr9 bool, atr10 bool,  atr11 int);" +
                "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                "cseEventStream[atr5 < 700 ] select atr3-atr11 as dividedVal, atr5 as threshold,  atr1 " +
                "as symbol, " + "cast(atr2,  'double') as priceInDouble,  sum(atr7) as summedValue insert" +
                " into OutStockStream ; end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(null, event.getData(0));
                        eventArrived = true;
                    }
                }
                eventArrived = true;
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", "mm", null, 101.0, 500L, 20L, 11.43, 75.7f, false, true, 23});
        inputHandler.send(new Object[]{"WSO2", "aa", null, 101.0, 501L, 206L, 15.21, 76.7f, false, true, 65});
        inputHandler.send(new Object[]{"IBM", "nn", null, 102.0, 502L, 202L, 45.23, 77.7f, false, true, 12});
        inputHandler.send(new Object[]{"ORACLE", "ss", null, 101.0, 502L, 209L, 87.34, 77.7f, false, false, 34});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        AssertJUnit.assertEquals(4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery50() throws InterruptedException {
        log.info("Partition test50");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('PartitionTest50') " +
                "" +
                "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "" +
                "define stream cseEventStreamOne (symbol string, price float,volume int);" +
                "define stream cseEventStreamTwo (symbol string, price float,volume int);" +
                "" +
                "@info(name = 'query')" +
                "from cseEventStreamOne " +
                "select symbol, price, volume " +
                "insert into cseEventStream;" +
                " " +
                "partition with (price>=100 as 'large' or " +
                "                price<100 and price>=50 as 'medium' or " +
                "                price<50 as 'small' of cseEventStream, " +
                "                symbol of cseEventStreamTwo ) " +
                "   begin" +
                "   @info(name = 'query1') " +
                "   from cseEventStream " +
                "   insert into #OutStockStream1 ; " +
                "" +
                "   @info(name = 'query2') " +
                "   from cseEventStreamTwo " +
                "   insert into #OutStockStream1 ; " +
                " " +
                "   @info(name = 'query3') " +
                "   from #OutStockStream1 " +
                "   select symbol, sum(price) as price " +
                "   insert into #OutStockStream2 ;" +
                " " +
                "   @info(name = 'query4') " +
                "   from #OutStockStream2 " +
                "   insert into OutStockStream ;" +
                " " +
                "   end ; ";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        AssertJUnit.assertEquals(30.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        AssertJUnit.assertEquals(7005.60009765625, event.getData()[1]);
                    } else if (count.get() == 4) {
                        AssertJUnit.assertEquals(50.0, event.getData()[1]);
                    } else if (count.get() == 5) {
                        AssertJUnit.assertEquals(55.0, event.getData()[1]);
                    } else if (count.get() == 6) {
                        AssertJUnit.assertEquals(7000.000097751617, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandlerOne = siddhiAppRuntime.getInputHandler("cseEventStreamOne");
        InputHandler inputHandlerTwo = siddhiAppRuntime.getInputHandler("cseEventStreamTwo");
        siddhiAppRuntime.start();

        inputHandlerOne.send(new Object[]{"IBM", 25f, 100});
        inputHandlerTwo.send(new Object[]{"small", 5f, 100});
        inputHandlerOne.send(new Object[]{"WSO2", 7005.6f, 100});
        inputHandlerOne.send(new Object[]{"IBM", 50f, 100});
        inputHandlerOne.send(new Object[]{"ORACLE", 25f, 100});
        inputHandlerTwo.send(new Object[]{"large", -5.6f, 100});

        SiddhiTestHelper.waitForEvents(100, 5, count, 60000);
        AssertJUnit.assertTrue(6 >= count.get());
        siddhiAppRuntime.shutdown();

    }

}

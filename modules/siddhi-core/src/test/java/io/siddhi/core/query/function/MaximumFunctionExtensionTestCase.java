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

package io.siddhi.core.query.function;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MaximumFunctionExtensionTestCase {
    private static final Logger log = Logger.getLogger(MaximumFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }


    @Test
    public void testMaxFunctionExtension1() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 double,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(36.75, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(38.12, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39.25, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(37.75, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(38.12, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(40.0, event.getData(0));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testMaxFunctionExtension2() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 int,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testMaxFunctionExtension3() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 int,price2 int, price3 int);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(74, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(78, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39, event.getData(0));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36, 38, 74});
        inputHandler.send(new Object[]{78, 38, 37});
        inputHandler.send(new Object[]{9, 39, 38});

        Thread.sleep(300);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testMaxFunctionExtension4() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 float, price2 float, price3 float);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(36.75f, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(38.12f, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39.25f, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(37.75f, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(38.12f, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(40.0f, event.getData(0));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testMaxFunctionExtension5() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 long, price2 long, price3 long);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(74L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(78L, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39L, event.getData(0));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36, 38, 74});
        inputHandler.send(new Object[]{78, 38, 37});
        inputHandler.send(new Object[]{9, 39, 38});

        Thread.sleep(300);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testMaxFunctionExtension6() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 6");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 double,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(*) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(36.75, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(38.12, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39.25, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(37.75, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(38.12, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(40.0, event.getData(0));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testMaxFunctionExtensionException7() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 7");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 object,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testMaxFunctionExtensionException8() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 8");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 double,price2 double, price3 object);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testMaxFunctionExtension9() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 9");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 float,price2 float, price3 float);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(36.75f, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(38.12f, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(39.25f, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(37.75f, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(38.12f, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(40.0f, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36f, 36.75f, 35.75f});
        inputHandler.send(new Object[]{37.88f, 38.12f, 37.62f});
        inputHandler.send(new Object[]{39.00f, 39.25f, 38.62f});
        inputHandler.send(new Object[]{36.88f, 37.75f, 36.75f});
        inputHandler.send(new Object[]{38.12f, 38.12f, 37.75f});
        inputHandler.send(new Object[]{38.12f, 40f, 37.75f});

        Thread.sleep(300);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testMaxFunctionExtension10() throws InterruptedException {
        log.info("MaximumFunctionExecutor TestCase 10");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 long,price2 long, price3 long);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select maximum(price1, price2, price3) as max " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(3675L, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(3812L, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(3925L, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(3775L, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(3812L, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(3812L, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{36L, 3675L, 3575L});
        inputHandler.send(new Object[]{3788L, 3812L, 3762L});
        inputHandler.send(new Object[]{3900L, 3925L, 3862L});
        inputHandler.send(new Object[]{3688L, 3775L, 3675L});
        inputHandler.send(new Object[]{3812L, 3812L, 3775L});
        inputHandler.send(new Object[]{3812L, 40L, 3775L});

        Thread.sleep(300);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }
}

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.query.function;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class MinimumFunctionExtensionTestCase {
    static final Logger log = Logger.getLogger(MinimumFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }


    @Test
    public void testMinFunctionExtension1() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 double,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select minimum(price1, price2, price3) as min " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(35.75, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37.62, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(38.62, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(36.75, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testMinFunctionExtension2() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 int,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select minimum(price1, price2, price3) as min " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testMinFunctionExtension3() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 int,price2 int, price3 int);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select minimum(price1, price2, price3) as min " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(36, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(9, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 38, 74});
        inputHandler.send(new Object[]{78, 38, 37});
        inputHandler.send(new Object[]{9, 39, 38});

        Thread.sleep(300);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testMinFunctionExtension4() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 float, price2 float, price3 float);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select minimum(price1, price2, price3) as min " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(35.75f, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37.62f, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(38.62f, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(36.75f, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(37.75f, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(37.75f, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testMinFunctionExtension5() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 long, price2 long, price3 long);";
        String query = ("@info(name = 'query1') from inputStream " +
                "select minimum(price1, price2, price3) as min " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(36l, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37l, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(9l, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 38, 74});
        inputHandler.send(new Object[]{78, 38, 37});
        inputHandler.send(new Object[]{9, 39, 38});

        Thread.sleep(300);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testMinFunctionExtensionException6() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 6");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 object,price2 double, price3 double);";
        String query = ("@info(name = 'query1') from inputStream " +
                        "select minimum(price1, price2, price3) as min " +
                        "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(35.75, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37.62, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(38.62, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(36.75, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testMinFunctionExtensionException7() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 7");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 double,price2 double, price3 object);";
        String query = ("@info(name = 'query1') from inputStream " +
                        "select minimum(price1, price2, price3) as min " +
                        "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(35.75, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37.62, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(38.62, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(36.75, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(37.75, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36, 36.75, 35.75});
        inputHandler.send(new Object[]{37.88, 38.12, 37.62});
        inputHandler.send(new Object[]{39.00, 39.25, 38.62});
        inputHandler.send(new Object[]{36.88, 37.75, 36.75});
        inputHandler.send(new Object[]{38.12, 38.12, 37.75});
        inputHandler.send(new Object[]{38.12, 40, 37.75});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testMinFunctionExtension8() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 8");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 float, price2 float, price3 float);";
        String query = ("@info(name = 'query1') from inputStream " +
                        "select minimum(price1, price2, price3) as min " +
                        "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(35.75f, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(37.62f, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(38.62f, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(36.75f, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(37.75f, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(37.75f, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36f, 36.75f, 35.75f});
        inputHandler.send(new Object[]{37.88f, 38.12f, 37.62f});
        inputHandler.send(new Object[]{39.00f, 39.25f, 38.62f});
        inputHandler.send(new Object[]{36.88f, 37.75f, 36.75f});
        inputHandler.send(new Object[]{38.12f, 38.12f, 37.75f});
        inputHandler.send(new Object[]{38.12f, 40f, 37.75f});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testMinFunctionExtension9() throws InterruptedException {
        log.info("MinimumFunctionExecutor TestCase 9");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price1 long,price2 long, price3 long);";
        String query = ("@info(name = 'query1') from inputStream " +
                        "select minimum(price1, price2, price3) as min " +
                        "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            Assert.assertEquals(36l, event.getData(0));
                            break;
                        case 2:
                            Assert.assertEquals(3762l, event.getData(0));
                            break;
                        case 3:
                            Assert.assertEquals(3862l, event.getData(0));
                            break;
                        case 4:
                            Assert.assertEquals(3675l, event.getData(0));
                            break;
                        case 5:
                            Assert.assertEquals(3775l, event.getData(0));
                            break;
                        case 6:
                            Assert.assertEquals(40l, event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{36l, 3675l, 3575l});
        inputHandler.send(new Object[]{3788l, 3812l, 3762l});
        inputHandler.send(new Object[]{3900l, 3925l, 3862l});
        inputHandler.send(new Object[]{3688l, 3775l, 3675l});
        inputHandler.send(new Object[]{3812l, 3812l, 3775l});
        inputHandler.send(new Object[]{3812l, 40l, 3775l});

        Thread.sleep(300);
        Assert.assertEquals(6, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

}

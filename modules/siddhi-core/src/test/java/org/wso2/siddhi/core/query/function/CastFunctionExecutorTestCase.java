/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class CastFunctionExecutorTestCase {
    private static final Logger log = Logger.getLogger(CastFunctionExecutorTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testCastFunctionExtension() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                + "cast(price, 'double') as priceInDouble insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100.3d, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300d, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100.3, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300d, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testCastFunctionExtensionException2() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, 'double','ddd') as priceInDouble insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100.3d, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300d, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100.3, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300d, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testCastFunctionExtensionException3() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, 'newType') as priceInDouble insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100.3d, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300d, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100.3, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300d, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testCastFunctionExtensionException4() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, symbol) as priceInDouble insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100.3d, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300d, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100.3, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300d, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCastFunctionExtension5() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, 'float') as priceInFloat insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100.3f, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300f, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100.3f, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300f, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCastFunctionExtension6() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, 'long') as priceInLong insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(1003453l, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(30043253l, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 1003453l, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 30043253l, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCastFunctionExtension7() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, isAllowed object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,isAllowed, "
                        + "cast(isAllowed, 'bool') as allowedInBool insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(false, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", true, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", false, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCastFunctionExtension8() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol object, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(symbol, 'string') as symbolInString insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals("IBM", event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("WSO2", event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("XYZ", event.getData(0));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 1003453l, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 30043253l, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCastFunctionExtension9() throws InterruptedException {
        log.info("CastFunctionExecutor TestCase 9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price object, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol,price, "
                        + "cast(price, 'int') as priceInInt insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(1003, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(2));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(300432, event.getData(2));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 1003, 100l});
        inputHandler.send(new Object[]{"WSO2", true, 200l});
        inputHandler.send(new Object[]{"XYZ", 300432, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

}

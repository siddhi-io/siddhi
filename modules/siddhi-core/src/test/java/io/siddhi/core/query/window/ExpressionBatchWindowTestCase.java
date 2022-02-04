/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.query.window;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExpressionBatchWindowTestCase {
    private static final Logger log = LogManager.getLogger(ExpressionBatchWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;
    private Event storedEvent;


    @BeforeMethod
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        storedEvent = null;
    }

    @Test
    public void expressionBatchWindowTest1() throws InterruptedException {
        log.info("Testing expression batch window with no of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch('count() <= 2') " +
                "select symbol, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(2, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(2, removeEvents.length);
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 62.5f, 3});
        inputHandler.send(new Object[]{"WSO2", 63.5f, 4});
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void expressionBatchWindowTest2() throws InterruptedException {
        log.info("Testing expression batch window 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch('last.volume - first.volume < 2') " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(2, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(2, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 60.5f, 0});
        inputHandler.send(new Object[]{"WSO2", 61.5f, 1});
        inputHandler.send(new Object[]{"WSO2", 62.5f, 2});
        inputHandler.send(new Object[]{"WSO2", 63.5f, 3});
        inputHandler.send(new Object[]{"WSO2", 64.5f, 4});
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionBatchWindowTest3() throws InterruptedException {
        log.info("Testing expression batch window 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch('eventTimestamp(last) - eventTimestamp(first) < 2') " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(2, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(2, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2});
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4});
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionBatchWindowTest4() throws InterruptedException {
        log.info("Testing expression batch window 4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch('eventTimestamp(last) - eventTimestamp(first) <= 2') " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(3, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(3, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Event[]{
                new Event(0, new Object[]{"WSO2", 60.5f, 0}),
                new Event(1, new Object[]{"WSO2", 61.5f, 1}),
                new Event(2, new Object[]{"WSO2", 62.5f, 2}),
                new Event(3, new Object[]{"WSO2", 63.5f, 3}),
                new Event(4, new Object[]{"WSO2", 64.5f, 4}),
                new Event(5, new Object[]{"WSO2", 65.5f, 5}),
                new Event(6, new Object[]{"WSO2", 66.5f, 6}),
        });
        AssertJUnit.assertEquals(6, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionBatchWindowTest5() throws InterruptedException {
        log.info("Testing expression batch window 5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                count++;
                AssertJUnit.assertEquals(2, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(2, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest6() throws InterruptedException {
        log.info("Testing expression batch window 6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(4, inEvents.length);
                count++;
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(4, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 4";
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr});
        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest7() throws InterruptedException {
        log.info("Testing expression batch window 7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length || 2 == inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length || 2 == removeEvents.length
                            || 1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest8() throws InterruptedException {
        log.info("Testing expression batch window 8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(1, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionBatchWindowTest9() throws InterruptedException {
        log.info("Testing expression batch window 9");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                count++;
                AssertJUnit.assertEquals(1, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(new Event[]{
                new Event(0, new Object[]{"WSO2", 60.5f, 0, expr}),
                new Event(1, new Object[]{"WSO2", 61.5f, 1, expr}),
                new Event(2, new Object[]{"WSO2", 62.5f, 2, expr})
        });
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest10() throws InterruptedException {
        log.info("Testing expression batch window 10");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertTrue(1 == inEvents.length || 2 == inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(1 == removeEvents.length || 2 == removeEvents.length
                            || 3 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr1 = "eventTimestamp(last) - eventTimestamp(first) < 2";
        String expr2 = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(new Event[]{
                new Event(0, new Object[]{"WSO2", 60.5f, 0, expr1}),
                new Event(1, new Object[]{"WSO2", 61.5f, 1, expr1}),
                new Event(2, new Object[]{"WSO2", 62.5f, 2, expr1}),
                new Event(3, new Object[]{"WSO2", 63.5f, 3, expr2}),
                new Event(4, new Object[]{"WSO2", 64.5f, 4, expr2}),
                new Event(5, new Object[]{"WSO2", 65.5f, 5, expr1}),
                new Event(6, new Object[]{"WSO2", 66.5f, 6, expr1}),
                new Event(7, new Object[]{"WSO2", 67.5f, 7, expr1}),
                new Event(8, new Object[]{"WSO2", 68.5f, 8, expr1}),
                new Event(9, new Object[]{"WSO2", 69.5f, 9, expr1})
        });

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest11() throws InterruptedException {
        log.info("Testing expression batch window 11");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                count++;
                AssertJUnit.assertTrue(2 == inEvents.length || 4 == inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(2 == removeEvents.length || 4 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 4";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr});

        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertEquals(6, removeEventCount);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest12() throws InterruptedException {
        log.info("Testing expression batch window 12");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                count++;
                AssertJUnit.assertEquals(4, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(4 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "sum(price) < 50";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 10.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 10.0, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 10.0, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 10.0, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 10.0, 8, expr});

        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest13() throws InterruptedException {
        log.info("Testing expression batch window 13");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                count++;
                AssertJUnit.assertTrue(3 == inEvents.length || 4 == inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length || 4 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "sum(price) < 40";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 10.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 10.0, 3, expr});
        expr = "sum(price) < 50";
        inputHandler.send(4, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 10.0, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 10.0, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 10.0, 8, expr});

        AssertJUnit.assertEquals(7, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void expressionBatchWindowTest14() throws InterruptedException {
        log.info("Testing expression batch window 14");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertTrue(1 == inEvents.length || 3 == inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(1 == removeEvents.length || 4 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "sum(price) < 40";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 10.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 10.0, 3, expr});
        expr = "sum(price) < 0";
        inputHandler.send(4, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 10.0, 5, expr});
        expr = "sum(price) < 40";
        inputHandler.send(6, new Object[]{"WSO2", 10.0, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 10.0, 8, expr});
        inputHandler.send(9, new Object[]{"WSO2", 10.0, 9, expr});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(6, removeEventCount);
        AssertJUnit.assertEquals(5, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest15() throws InterruptedException {
        log.info("Testing expression batch window 15");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr) " +
                "select symbol, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertTrue(3 == inEvents.length || 2 == inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length || 2 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "sum(price) < 30 and eventTimestamp(last) - eventTimestamp(first) < 3";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 5.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 3, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 5, expr});
        inputHandler.send(8, new Object[]{"WSO2", 15.0, 6, expr});
        inputHandler.send(9, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(10, new Object[]{"WSO2", 10.0, 8, expr});

        AssertJUnit.assertEquals(7, inEventCount);
        AssertJUnit.assertEquals(5, removeEventCount);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest16() throws InterruptedException {
        log.info("Testing expression batch window 16");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, true) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length || 2 == inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length || 2 == removeEvents.length
                            || 1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 1";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 1";
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest17() throws InterruptedException {
        log.info("Testing expression batch window 17");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " expr string, include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, include) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length || 2 == inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length || 2 == removeEvents.length
                            || 1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        Boolean include = true;
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 1";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr, include});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr, include});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr, include});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        include = false;
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr, include});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr, include});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr, include});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr, include});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr, include});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest18() throws InterruptedException {
        log.info("Testing expression batch window 18");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " expr string, include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, include, true) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(2 == removeEvents.length || 1 == removeEvents.length
                            || 1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        Boolean include = true;
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 1";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr, include});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr, include});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr, include});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        include = false;
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr, include});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr, include});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr, include});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr, include});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr, include});

        AssertJUnit.assertEquals(10, inEventCount);
        AssertJUnit.assertEquals(9, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest19() throws InterruptedException {
        log.info("Testing expression batch window 19");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " expr string, include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, include, true) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(2 == removeEvents.length || 1 == removeEvents.length
                            || 1 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        Boolean include = true;
        String expr = "eventTimestamp(last)";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr, include});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr, include});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr, include});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        include = false;
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr, include});
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr, include});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr, include});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr, include});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr, include});

        AssertJUnit.assertEquals(7, inEventCount);
        AssertJUnit.assertEquals(6, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void expressionBatchWindowTest20() throws InterruptedException {
        log.info("Testing expression batch window 20");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch('eventTimestamp(last)', include, true) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest21() throws InterruptedException {
        log.info("Testing expression batch window 21");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " expr string, include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, include, true) " +
                "select symbol, price, count() as totalCount " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length);
                    for (Event event : inEvents) {
                        AssertJUnit.assertTrue(1 == (Long) event.getData(2)
                                || 2 == (Long) event.getData(2)
                                || 0 == (Long) event.getData(2));
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(1 == removeEvents.length);
                    for (Event event : removeEvents) {
                        AssertJUnit.assertTrue(0 == (Long) event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        Boolean include = true;
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr, include});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr, include});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr, include});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr, include});
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr, include});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr, include});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr, include});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr, include});

        AssertJUnit.assertEquals(8, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionBatchWindowTest22() throws InterruptedException {
        log.info("Testing expression batch window 22");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int," +
                " expr string, include bool);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expressionBatch(expr, include, true) " +
                "select symbol, price, count() as totalCount " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                count++;
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertTrue(1 == inEvents.length);
                    for (Event event : inEvents) {
                        AssertJUnit.assertTrue(1 == (Long) event.getData(2)
                                || 2 == (Long) event.getData(2)
                                || 0 == (Long) event.getData(2));
                    }
                }
                if (removeEvents != null) {
                    AssertJUnit.fail("remove events are not expected");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        Boolean include = false;
        String expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr, include});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr, include});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 0";
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr, include});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr, include});
        inputHandler.send(5, new Object[]{"WSO2", 65.5f, 5, expr, include});
        expr = "eventTimestamp(last) - eventTimestamp(first) < 2";
        inputHandler.send(6, new Object[]{"WSO2", 66.5f, 6, expr, include});
        inputHandler.send(7, new Object[]{"WSO2", 67.5f, 7, expr, include});
        inputHandler.send(8, new Object[]{"WSO2", 68.5f, 8, expr, include});
        inputHandler.send(9, new Object[]{"WSO2", 69.5f, 9, expr, include});

        AssertJUnit.assertEquals(10, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

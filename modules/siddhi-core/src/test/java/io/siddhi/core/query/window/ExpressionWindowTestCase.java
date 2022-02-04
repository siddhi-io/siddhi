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
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExpressionWindowTestCase {
    private static final Logger log = LogManager.getLogger(ExpressionWindowTestCase.class);
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
    public void expressionWindowTest1() throws InterruptedException {
        log.info("Testing expression window with no of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.expression('count() <= 2') " +
                "select symbol, volume " +
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
                    AssertJUnit.assertEquals(1, removeEvents.length);
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
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void expressionWindowTest2() throws InterruptedException {
        log.info("Testing expression window 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression('last.volume - first.volume <= 2') " +
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
                    AssertJUnit.assertEquals(1, removeEvents.length);
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
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionWindowTest3() throws InterruptedException {
        log.info("Testing expression window 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression('eventTimestamp(last) - eventTimestamp(first) <= 2') " +
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
                    AssertJUnit.assertEquals(1, removeEvents.length);
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
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionWindowTest4() throws InterruptedException {
        log.info("Testing expression window 4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression('eventTimestamp(last) - eventTimestamp(first) <= 2') " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(7, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertEquals(4, removeEvents.length);
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
        AssertJUnit.assertEquals(7, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void expressionWindowTest5() throws InterruptedException {
        log.info("Testing expression window 5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
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
                    AssertJUnit.assertEquals(1, removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "eventTimestamp(last) - eventTimestamp(first) <= 2";
        inputHandler.send(0, new Object[]{"WSO2", 60.5f, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 61.5f, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 62.5f, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 63.5f, 3, expr});
        inputHandler.send(4, new Object[]{"WSO2", 64.5f, 4, expr});
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest6() throws InterruptedException {
        log.info("Testing expression window 6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
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
                    AssertJUnit.assertEquals(1, removeEvents.length);
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
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(1, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest7() throws InterruptedException {
        log.info("Testing expression window 7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
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
                    AssertJUnit.assertTrue(1 == removeEvents.length || 3 == removeEvents.length);
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

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest8() throws InterruptedException {
        log.info("Testing expression window 8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
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
    public void expressionWindowTest9() throws InterruptedException {
        log.info("Testing expression window 9");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(3, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(3 == removeEvents.length);
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
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest10() throws InterruptedException {
        log.info("Testing expression window 10");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount = inEventCount + inEvents.length;
                AssertJUnit.assertEquals(9, inEvents.length);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    AssertJUnit.assertTrue(7 == removeEvents.length);
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
                new Event(8, new Object[]{"WSO2", 68.5f, 8, expr1})
        });

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest11() throws InterruptedException {
        log.info("Testing expression window 11");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
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
                    AssertJUnit.assertTrue(1 == removeEvents.length || 3 == removeEvents.length);
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

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest12() throws InterruptedException {
        log.info("Testing expression window 12");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, volume " +
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

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(5, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest13() throws InterruptedException {
        log.info("Testing expression window 13");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, volume " +
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
        String expr = "sum(price) < 30";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 10.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 10.0, 3, expr});
        expr = "sum(price) < 40";
        inputHandler.send(4, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 10.0, 5, expr});
        inputHandler.send(6, new Object[]{"WSO2", 10.0, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 10.0, 8, expr});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(6, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void expressionWindowTest14() throws InterruptedException {
        log.info("Testing expression window 14");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, volume " +
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
                    AssertJUnit.assertTrue(1 == removeEvents.length || 3 == removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        String expr = "sum(price) < 30";
        inputHandler.send(0, new Object[]{"WSO2", 10.0, 0, expr});
        inputHandler.send(1, new Object[]{"WSO2", 10.0, 1, expr});
        inputHandler.send(2, new Object[]{"WSO2", 10.0, 2, expr});
        inputHandler.send(3, new Object[]{"WSO2", 10.0, 3, expr});
        expr = "sum(price) < 0";
        inputHandler.send(4, new Object[]{"WSO2", 10.0, 4, expr});
        inputHandler.send(5, new Object[]{"WSO2", 10.0, 5, expr});
        expr = "sum(price) < 30";
        inputHandler.send(6, new Object[]{"WSO2", 10.0, 6, expr});
        inputHandler.send(7, new Object[]{"WSO2", 10.0, 7, expr});
        inputHandler.send(8, new Object[]{"WSO2", 10.0, 8, expr});

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expressionWindowTest15() throws InterruptedException {
        log.info("Testing expression window 15");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume int, expr string);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.expression(expr) " +
                "select symbol, volume " +
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
                    AssertJUnit.assertTrue(1 == removeEvents.length || 2 == removeEvents.length);
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

        AssertJUnit.assertEquals(9, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

}

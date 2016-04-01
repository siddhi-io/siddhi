/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.eventtable.hazelcast;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.eventtable.test.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.DuplicateDefinitionException;

import java.util.concurrent.atomic.AtomicInteger;

public class InsertOverwriteTableTestCase {
    private static final Logger log = Logger.getLogger(InsertOverwriteTableTestCase.class);
    private static final long RESULT_WAIT = 500;
    private AtomicInteger inEventCount = new AtomicInteger(0);
    private AtomicInteger removeEventCount = new AtomicInteger(0);
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount.set(0);
        removeEventCount.set(0);
        eventArrived = false;
    }

    @Test
    public void insertOverwriteTableTest1() throws InterruptedException {
        log.info("insertOverwriteTableTest1");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT011 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT011 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "insert overwrite StockTableT011 " +
                "   on StockTableT011.symbol=='IBM' ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 75.6f, 100l});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100l});
            updateStockStream.send(new Object[]{"GOOG", 10.6f, 100l});
            Thread.sleep(RESULT_WAIT);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest2() throws InterruptedException {
        log.info("insertOverwriteTableTest2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT021 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "insert overwrite StockTableT021 " +
                "   on StockTableT021.symbol==symbol ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 75.6f, 100l});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100l});
            stockStream.send(new Object[]{"WSO2", 10f, 100l});
            Thread.sleep(RESULT_WAIT);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest3() throws InterruptedException {
        log.info("insertOverwriteTableTest3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT031 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT031 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "insert overwrite StockTableT031 " +
                "   on StockTableT031.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT031.symbol" +
                " and  volume==StockTableT031.volume) in StockTableT031] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                case 3:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(3, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 55.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});
            updateStockStream.send(new Object[]{"IBM", 77.6f, 200l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 3, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest4() throws InterruptedException {
        log.info("insertOverwriteTableTest4");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT041 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "insert overwrite StockTableT041 " +
                "   on StockTableT041.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT041.symbol" +
                " and  volume==StockTableT041.volume) in StockTableT041] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                case 3:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(3, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 55.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});
            stockStream.send(new Object[]{"IBM", 77.6f, 200l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 3, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test(expected = DuplicateDefinitionException.class)
    public void insertOverwriteTableTest5() throws InterruptedException {
        log.info("insertOverwriteTableTest5");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT051 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT051 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol, vol as volume " +
                "insert overwrite StockTableT051 " +
                "   on StockTableT051.symbol==symbol;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 55.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});
            updateStockStream.send(new Object[]{"FB", 300l});
            checkStockStream.send(new Object[]{"FB", 300l});
            checkStockStream.send(new Object[]{"WSO2", 100l});

            Thread.sleep(RESULT_WAIT);
            Assert.assertEquals("Number of success events", 0, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", false, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest6() throws InterruptedException {
        log.info("insertOverwriteTableTest6");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT061 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert overwrite StockTableT061 " +
                "   on StockTableT061.symbol==symbol;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol, 0f as price, vol as volume " +
                "insert overwrite StockTableT061 " +
                "   on StockTableT061.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT061.symbol" +
                " and  volume==StockTableT061.volume) in StockTableT061] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                case 3:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 100l}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(3, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 55.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});
            updateStockStream.send(new Object[]{"IBM", 200l});
            updateStockStream.send(new Object[]{"FB", 300l});
            checkStockStream.send(new Object[]{"IBM", 100l});
            checkStockStream.send(new Object[]{"WSO2", 100l});

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 3, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }


    @Test
    public void insertOverwriteTableTest7() throws InterruptedException {
        log.info("insertOverwriteTableTest7");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT071 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT071 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol,  0f as price, vol as volume " +
                "insert overwrite StockTableT071 " +
                "   on StockTableT071.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT071.symbol and volume==StockTableT071.volume" +
                " and price==StockTableT071.price) in StockTableT071] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l, 155.6f}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 200l, 0f}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(2, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 155.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
            updateStockStream.send(new Object[]{"IBM", 200l});
            checkStockStream.send(new Object[]{"IBM", 200l, 0f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 2, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest8() throws InterruptedException {
        log.info("insertOverwriteTableTest8");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT081 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "select symbol, price, volume " +
                "insert overwrite StockTableT081 " +
                "   on StockTableT081.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT081.symbol and volume==StockTableT081.volume" +
                " and price==StockTableT081.price) in StockTableT081] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l, 155.6f}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 200l, 155.6f}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(2, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 155.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
            stockStream.send(new Object[]{"IBM", 155.6f, 200l});
            checkStockStream.send(new Object[]{"IBM", 200l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 2, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest9() throws InterruptedException {
        log.info("insertOverwriteTableTest9");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT091 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT091 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream left outer join StockTableT091 " +
                "   on UpdateStockStream.comp == StockTableT091.symbol " +
                "select symbol, ifThenElse(price is null,0f,price) as price, vol as volume " +
                "insert overwrite StockTableT091 " +
                "   on StockTableT091.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT091.symbol and volume==StockTableT091.volume" +
                " and price==StockTableT091.price) in StockTableT091] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 100l, 155.6f}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 200l, 155.6f}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(2, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            stockStream.send(new Object[]{"IBM", 155.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
            updateStockStream.send(new Object[]{"IBM", 200l});
            checkStockStream.send(new Object[]{"IBM", 200l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 2, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertOverwriteTableTest10() throws InterruptedException {
        log.info("insertOverwriteTableTest10");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Plan:name('InsertOverwriteTableExecutionPlan')" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@from(eventtable = 'hazelcast') " +
                "define table StockTableT101 (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTableT101 ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream left outer join StockTableT101 " +
                "   on UpdateStockStream.comp == StockTableT101.symbol " +
                "select comp as symbol, ifThenElse(price is null,0f,price) as price, vol as volume " +
                "insert overwrite StockTableT101 " +
                "   on StockTableT101.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTableT101.symbol and volume==StockTableT101.volume" +
                " and price==StockTableT101.price) in StockTableT101] " +
                "insert into OutStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount.incrementAndGet();
                            switch (inEventCount.get()) {
                                case 1:
                                    Assert.assertArrayEquals(new Object[]{"IBM", 200l, 0f}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertArrayEquals(new Object[]{"WSO2", 300l, 55.6f}, event.getData());
                                    break;
                                default:
                                    Assert.assertSame(2, inEventCount.get());
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount.addAndGet(removeEvents.length);
                    }
                    eventArrived = true;
                }
            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");

            executionPlanRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100l});
            checkStockStream.send(new Object[]{"IBM", 100l, 155.6f});
            checkStockStream.send(new Object[]{"WSO2", 100l, 155.6f});
            updateStockStream.send(new Object[]{"IBM", 200l});
            updateStockStream.send(new Object[]{"WSO2", 300l});
            checkStockStream.send(new Object[]{"IBM", 200l, 0f});
            checkStockStream.send(new Object[]{"WSO2", 300l, 55.6f});

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            Assert.assertEquals("Number of success events", 2, inEventCount.get());
            Assert.assertEquals("Number of remove events", 0, removeEventCount.get());
            Assert.assertEquals("Event arrived", true, eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }
}

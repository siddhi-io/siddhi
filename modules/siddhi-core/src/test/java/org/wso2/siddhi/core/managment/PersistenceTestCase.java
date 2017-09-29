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

package org.wso2.siddhi.core.managment;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.util.concurrent.atomic.AtomicInteger;

public class PersistenceTestCase {
    static final Logger log = Logger.getLogger(PersistenceTestCase.class);
    private int count;
    private AtomicInteger atomicCount;
    private boolean eventArrived;
    private long firstValue;
    private long lastValue;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
        firstValue = 0;
        lastValue = 0;
        atomicCount = new AtomicInteger(0);
    }

    @Test
    public void persistenceTest1() throws InterruptedException {
        log.info("persistence test 1 - window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.length(10) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        Thread.sleep(100);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(200, lastValue);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertTrue(count <= 6);
        Assert.assertEquals(400, lastValue);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest2() throws InterruptedException {
        log.info("persistence test 2 - pattern count query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";


        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertArrayEquals(new Object[]{25.6f, 47.6f, null, null, 45.7f}, inEvent.getData());
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2;
        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 13.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, count);
        Assert.assertEquals("Event arrived", false, eventArrived);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        stream1 = executionPlanRuntime.getInputHandler("Stream1");
        stream2 = executionPlanRuntime.getInputHandler("Stream2");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(500);
        stream1.send(new Object[]{"GOOG", 47.8f, 100});
        Thread.sleep(500);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(500);

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals("Number of success events", 1, count);
        Assert.assertEquals("Event arrived", true, eventArrived);

    }

    @Test(expected = NoPersistenceStoreException.class)
    public void persistenceTest3() throws InterruptedException {
        log.info("persistence test 3 - no store defined");

        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";


        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertArrayEquals(new Object[]{25.6f, 47.6f, null, null, 45.7f}, inEvent.getData());
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 13.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, count);
        Assert.assertEquals("Event arrived", false, eventArrived);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void persistenceTest4() throws InterruptedException {
        log.info("persistence test 4 - window restart");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.time(10 sec) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        Thread.sleep(100);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(200, lastValue);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(400, lastValue);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest5() throws InterruptedException {
        log.info("persistence test 5 - window restart expired event ");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.time(10 sec) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert all events into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        count++;
                        Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                        firstValue = (Long) inEvent.getData(2);
                    }
                }
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        count++;
                        lastValue = (Long) removeEvent.getData(2);
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        Thread.sleep(100);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(firstValue, 200);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        //shutdown execution plan
        Thread.sleep(15000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(400, firstValue);
        Assert.assertEquals(0, lastValue);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest6() throws InterruptedException {
        log.info("persistence test 6 - batch window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.timeBatch(10) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(2, count);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 6);
        Assert.assertEquals(true, eventArrived);

    }


    @Test
    public void persistenceTest7() throws InterruptedException {
        log.info("persistence test 7 - external time window with group by query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream (symbol string, price float, volume int, timestamp long);" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream#window.externalTime(timestamp,3 sec) " +
                "select symbol, price, sum(volume) as totalVol, timestamp " +
                "group by symbol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    if (count == 5) {
                        Assert.assertEquals(300l, inEvent.getData(2));
                    }
                    if (count == 6) {
                        Assert.assertEquals(100l, inEvent.getData(2));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();
        long currentTime = 0;

        inputHandler.send(new Object[]{"IBM", 75.1f, 100, currentTime + 1000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.2f, 100, currentTime + 2000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"IBM", 75.3f, 100, currentTime + 3000});

        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.4f, 100, currentTime + 4000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"IBM", 75.5f, 100, currentTime + 5000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, currentTime + 6000});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 6);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest8() throws InterruptedException {
        log.info("persistence test 8 - external time window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.externalTimeBatch(timestamp, 10) " +
                "select symbol, price, sum(volume) as totalVol " +
                "group by symbol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    if (count == 2) {
                        Assert.assertEquals(200L, inEvent.getData(2));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        long startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, startTime});

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.6f, 300, startTime + 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 400, startTime + 100});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 2);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest9() throws InterruptedException {
        log.info("persistence test 9 - external time window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.uniqueExternalTimeBatch(symbol, timestamp, 10) " +
                "select symbol, price, sum(volume) as totalVol " +
                "group by symbol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    if (count == 2) {
                        Assert.assertEquals(100L, inEvent.getData(2));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        long startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.8f, 100, startTime});

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.6f, 300, startTime + 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 400, startTime + 100});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 2);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest10() throws InterruptedException {
        log.info("persistence test 10 - external time window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(3) " +
                "select symbol, price, sum(volume) as totalVol " +
                "group by symbol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    if (count == 2) {
                        Assert.assertEquals(200L, inEvent.getData(2));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        long startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, startTime});
        inputHandler.send(new Object[]{"WSO2", 75.8f, 100, startTime});

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.6f, 300, startTime + 100});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 2);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest11() throws InterruptedException {
        log.info("persistence test 4 - window restart");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.timeLength(10 sec, 10) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        Thread.sleep(100);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(200, lastValue);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(400, lastValue);
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest12() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String cseEventStream = "@plan:name('Test') "
                + "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.cron('*/3 * * * * ?') "
                + "select symbol,price,volume insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        atomicCount.getAndIncrement();
                        Assert.assertTrue("IBM".equals(removeEvent.getData(0)) || "WSO2".equals(removeEvent.getData(0)));
                    }
                }
            }
        };
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});

        //persisting
        Thread.sleep(3000);
        executionPlanRuntime.persist();
        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("query1", queryCallback);
        executionPlanRuntime.getInputHandler("cseEventStream");
        //loading
        log.info("Restarting execution plan");
        executionPlanRuntime.restoreLastRevision();
        executionPlanRuntime.start();

        SiddhiTestHelper.waitForEvents(100, 2, atomicCount, 10000);
        Assert.assertEquals(2, atomicCount.intValue());

    }

    @Test
    public void persistenceStdDevTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, stddev(price) as stdPrice, stddev(volume) as stdVol, stddev(timestamp) as "
                + "stdTimestamp, stddev(marketIndex) "
                + "as stdmarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    Assert.assertTrue("WSO3".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    Assert.assertEquals(0.5, inEvent.getData(1));
                    Assert.assertEquals(50.0, inEvent.getData(2));
                    Assert.assertEquals(0.0, inEvent.getData(3));
                    Assert.assertEquals(1.5, inEvent.getData(4));
                }
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        Assert.assertTrue("WSO2".equals(removeEvent.getData(0)));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        long startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{"WSO2", 75f, 100, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO2", 76f, 200, startTime, 5.3});


        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO3", 77f, 300, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO3", 78f, 400, startTime, 5.3});

        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 3, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(3, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceMaxDevTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, max(price) as maxPrice, max(volume) as maxVol, max(timestamp) as "
                + "maxTimestamp, max(marketIndex) "
                + "as maxMarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";
        final long startTime = System.currentTimeMillis();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    Assert.assertTrue("WSO3".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    Assert.assertEquals(76F, inEvent.getData(1));
                    Assert.assertEquals(200, inEvent.getData(2));
                    Assert.assertEquals(startTime, inEvent.getData(3));
                    Assert.assertEquals(5.3, inEvent.getData(4));
                }
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        Assert.assertTrue("WSO2".equals(removeEvent.getData(0)));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();


        inputHandler.send(new Object[]{"WSO2", 75f, 100, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO2", 76f, 200, startTime, 5.3});


        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO3", 75f, 100, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO3", 76f, 200, startTime, 5.3});

        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 3, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(3, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceMinDevTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, min(price) as minPrice, min(volume) as minVol, min(timestamp) as "
                + "minTimestamp, min(marketIndex) "
                + "as minMarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";
        final long startTime = System.currentTimeMillis();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    Assert.assertTrue("WSO3".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    Assert.assertEquals(75F, inEvent.getData(1));
                    Assert.assertEquals(100, inEvent.getData(2));
                    Assert.assertEquals(startTime, inEvent.getData(3));
                    Assert.assertEquals(2.3, inEvent.getData(4));
                }
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        Assert.assertTrue("WSO2".equals(removeEvent.getData(0)));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();


        inputHandler.send(new Object[]{"WSO2", 75f, 100, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO2", 76f, 200, startTime, 5.3});


        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO3", 75f, 100, startTime, 2.3});
        inputHandler.send(new Object[]{"WSO3", 76f, 200, startTime, 5.3});

        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 3, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(3, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceAvgDevTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, avg(price) as avgPrice, avg(volume) as avgVol, avg(timestamp) as "
                + "avgTimestamp, avg(marketIndex) "
                + "as avgMarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";
        final long startTime = System.currentTimeMillis();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    if (atomicCount.get() == 1) {
                        Assert.assertTrue("WSO2".equals(inEvent.getData(0)));
                        Assert.assertEquals(75.0, inEvent.getData(1));
                        Assert.assertEquals(150.0, inEvent.getData(2));
                        Assert.assertEquals(Double.parseDouble(String.valueOf(startTime)), inEvent.getData(3));
                        Assert.assertEquals(3.0, inEvent.getData(4));
                    } else {
                        Assert.assertTrue("WSO3".equals(inEvent.getData(0)));
                        Assert.assertEquals(85.0, inEvent.getData(1));
                        Assert.assertEquals(250.0, inEvent.getData(2));
                        Assert.assertEquals(Double.parseDouble(String.valueOf(startTime)), inEvent.getData(3));
                        Assert.assertEquals(3.0, inEvent.getData(4));
                    }
                }
                if (removeEvents != null) {
                    for (Event removeEvent : removeEvents) {
                        Assert.assertTrue("WSO2".equals(removeEvent.getData(0)));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();


        inputHandler.send(new Object[]{"WSO2", 74f, 100, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO2", 76f, 200, startTime, 3.5});


        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO3", 84f, 200, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO3", 86f, 300, startTime, 3.5});

        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 3, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(3, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceMaxForeverTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, maxForever(price) as maxForeverPrice, maxForever(volume) as maxForeverVol, "
                + "maxForever(timestamp) as maxForeverTimestamp, maxForever(marketIndex) "
                + "as maxForeverMarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";
        final long startTime = System.currentTimeMillis();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    if ("WSO2".equals(inEvent.getData(0))) {
                        Assert.assertEquals(84.0F, inEvent.getData(1));
                        Assert.assertEquals(200, inEvent.getData(2));
                        Assert.assertEquals(startTime, inEvent.getData(3));
                        Assert.assertEquals(2.5, inEvent.getData(4));
                    } else {
                        Assert.assertTrue("WSO3".equals(inEvent.getData(0)));
                        Assert.assertEquals(86.0F, inEvent.getData(1));
                        Assert.assertEquals(300, inEvent.getData(2));
                        Assert.assertEquals(startTime, inEvent.getData(3));
                        Assert.assertEquals(3.5, inEvent.getData(4));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 84f, 200, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO3", 86f, 300, startTime, 3.5});


        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO2", 74f, 100, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO3", 76f, 200, startTime, 3.5});
        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 4, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(4, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceMinForeverTest() throws InterruptedException {

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int, timestamp long, marketIndex "
                + "double );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(2) " +
                "select symbol, minForever(price) as minForeverPrice, minForever(volume) as minForeverVol, "
                + "minForever(timestamp) as minForeverTimestamp, minForever(marketIndex) "
                + "as minForeverMarketIndex"
                + " " +
                "group by symbol " +
                "insert all events into OutStream ";
        final long startTime = System.currentTimeMillis();
        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                atomicCount.addAndGet(inEvents == null ? 0 : inEvents.length);
                atomicCount.addAndGet(removeEvents == null ? 0 : removeEvents.length);
                for (Event inEvent : inEvents) {
                    if ("WSO2".equals(inEvent.getData(0))) {
                        Assert.assertEquals(74.0F, inEvent.getData(1));
                        Assert.assertEquals(100, inEvent.getData(2));
                        Assert.assertEquals(startTime, inEvent.getData(3));
                        Assert.assertEquals(2.5, inEvent.getData(4));
                    } else {
                        Assert.assertTrue("WSO3".equals(inEvent.getData(0)));
                        Assert.assertEquals(76.0F, inEvent.getData(1));
                        Assert.assertEquals(200, inEvent.getData(2));
                        Assert.assertEquals(startTime, inEvent.getData(3));
                        Assert.assertEquals(3.5, inEvent.getData(4));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 74f, 100, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO3", 76f, 200, startTime, 3.5});

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"WSO2", 84f, 200, startTime, 2.5});
        inputHandler.send(new Object[]{"WSO3", 86f, 300, startTime, 3.5});
        //shutdown execution plan
        SiddhiTestHelper.waitForEvents(100, 4, atomicCount, 2000);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(4, atomicCount.intValue());
        Assert.assertEquals(true, eventArrived);

    }

    @Test
    public void persistenceTest13() throws InterruptedException {
        log.info("persistenceTest8");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        String executionPlan = "" +
                "@Plan:name('SnapshotOutputRateLimitTest1') " +
                "" +
                "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.length(5) " +
                "select ip " +
                "output snapshot every 2 sec " +
                "insert into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        log.info("Running : " + executionPlanRuntime.getName());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 3) {
                        Assert.assertEquals("192.10.1.3", inEvent.getData(0));
                    }
                    if (count == 4) {
                        Assert.assertEquals("192.10.1.4", inEvent.getData(0));
                    }
                }
            }
        };

        // start, persist and shutdown
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.1"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.2"});
        Thread.sleep(500);
        executionPlanRuntime.persist();
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        // restore
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();
        executionPlanRuntime.restoreLastRevision();
        Thread.sleep(20);
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});

        // shutdown execution plan
        Thread.sleep(3000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(4, count);
        Assert.assertEquals(true, eventArrived);
    }

    /* TODO: Fix - https://github.com/wso2/siddhi/issues/551
    @Test()
    public void persistenceTest14() throws InterruptedException {
        log.info("persistenceTest14");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        String executionPlan = "" +
                "@Plan:name('SnapshotOutputRateLimitTest1') " +
                "" +
                "define stream LoginEvents (timeStamp long, ip string);" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.length(5) " +
                "select count() as count " +
                "group by ip " +
                "output snapshot every 2 sec " +
                "insert into ipCount ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        log.info("Running : " + executionPlanRuntime.getName());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 3) {
                        Assert.assertEquals("192.10.1.3", inEvent.getData(0));
                    }
                    if (count == 4) {
                        Assert.assertEquals("192.10.1.4", inEvent.getData(0));
                    }
                }
            }
        };

        // start, persist and shutdown
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.1"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.2"});
        Thread.sleep(500);
        executionPlanRuntime.persist();
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        // restore
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();
        executionPlanRuntime.restoreLastRevision();
        Thread.sleep(20);
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});

        // shutdown execution plan
        Thread.sleep(3000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(4, count);
        Assert.assertEquals(true, eventArrived);
    }
    */
}

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

package io.siddhi.core.managment;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IncrementalPersistenceTestCase {
    private static final Logger log = LogManager.getLogger(IncrementalPersistenceTestCase.class);
    private int count;
    private boolean eventArrived;
    private Long lastValue;
    private AtomicInteger inEventCount = new AtomicInteger(0);
    private int removeEventCount;
    private List<Object[]> inEventsList;
    private AtomicInteger count2 = new AtomicInteger(0);
    private String storageFilePath = "./target/temp";

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        removeEventCount = 0;
        lastValue = 0L;
        inEventsList = new ArrayList<Object[]>();
        inEventCount.set(0);
    }

    @Test
    public void incrementalPersistenceTest1() throws InterruptedException {
        log.info("Incremental persistence test 1 - length window query");
        final int inputEventCount = 10;
        final int eventWindowSize = 4;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest1') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.length(" + eventWindowSize + ") " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{"IBM", 75.6f + i, 100});
        }
        Thread.sleep(100);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(400), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"IBM", 100.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 200.4f, 100});

        inputHandler.send(new Object[]{"IBM", 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 400.4f, 200});
        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"IBM", 500.6f, 300});
        inputHandler.send(new Object[]{"WSO2", 600.6f, 400});

        //shutdown Siddhi app
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 6));
        AssertJUnit.assertEquals(new Long(1000), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest2() throws InterruptedException {
        log.info("Incremental persistence test 2 - length batch window query");
        final int inputEventCount = 10;
        final int eventWindowSize = 4;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest2') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.lengthBatch(" + eventWindowSize + ") " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert all events into OutStream";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{"IBM", 75.6f + i, 100});
        }
        Thread.sleep(100);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(400), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"WSO2", 100.4f, 150});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 200.4f, 110});

        inputHandler.send(new Object[]{"IBM", 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 400.4f, 300});

        inputHandler.send(new Object[]{"IBM", 500.6f, 120});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 600.6f, 400});

        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed", e);
        }
        siddhiAppRuntime.start();

        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 700.6f, 230});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 800.6f, 125});

        inputHandler.send(new Object[]{"IBM", 900.6f, 370});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 1000.6f, 140});

        //shutdown Siddhi app
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 6));
        AssertJUnit.assertEquals(new Long(865), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest3() throws InterruptedException {
        log.info("Incremental persistence test 3 - time window query");
        final int inputEventCount = 10;
        final int eventWindowSizeInSeconds = 2;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest3') " +
                "@app:playback " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.time(" + eventWindowSizeInSeconds + " sec) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                    log.info("last value: " + lastValue);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(i, new Object[]{"IBM", 75.6f + i, 100});
        }
        Thread.sleep(4000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(1000), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(3000, new Object[]{"WSO2", 100.4f, 150});
        //Thread.sleep(100);
        inputHandler.send(3010, new Object[]{"WSO2", 200.4f, 110});

        inputHandler.send(3020, new Object[]{"IBM", 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(3030, new Object[]{"WSO2", 400.4f, 300});

        inputHandler.send(3040, new Object[]{"IBM", 500.6f, 120});
        Thread.sleep(10);
        inputHandler.send(3050, new Object[]{"WSO2", 600.6f, 400});

        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        try {
            //loading
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();

        Thread.sleep(500);
        inputHandler.send(3500, new Object[]{"IBM", 700.6f, 230});
        Thread.sleep(10);
        inputHandler.send(3540, new Object[]{"WSO2", 800.6f, 125});

        inputHandler.send(3590, new Object[]{"IBM", 900.6f, 370});
        Thread.sleep(10);
        inputHandler.send(3600, new Object[]{"WSO2", 1000.6f, 140});

        //shutdown Siddhi app
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 10));
        AssertJUnit.assertEquals(new Long(2045), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest4() throws InterruptedException {
        log.info("Incremental persistence test 4 - time batch window query");
        final int inputEventCount = 10;
        final int eventWindowSizeInSeconds = 2;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest4') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.timeBatch(" + eventWindowSizeInSeconds + " sec) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                    log.info("last value: " + lastValue);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{"IBM", 75.6f + i, 100});
        }
        Thread.sleep(4000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(1000), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"WSO2", 100.4f, 150});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 200.4f, 110});

        inputHandler.send(new Object[]{"IBM", 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 400.4f, 300});

        inputHandler.send(new Object[]{"IBM", 500.6f, 120});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 600.6f, 400});

        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();

        Thread.sleep(50);
        inputHandler.send(new Object[]{"IBM", 700.6f, 230});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 800.6f, 125});

        inputHandler.send(new Object[]{"IBM", 900.6f, 370});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 1000.6f, 140});

        //shutdown Siddhi app
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 10));
        AssertJUnit.assertEquals(new Long(2045), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest4_1() throws InterruptedException {
        log.info("Incremental persistence test 4_1 - time batch window query");
        final int inputEventCount = 10;
        final int eventWindowSizeInSeconds = 2;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest4') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.timeBatch(" + eventWindowSizeInSeconds + " sec) " +
                "select symbol, price, sum(volume) as totalVol " +
                "insert into OutStream";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = (Long) inEvent.getData(2);
                    log.info("last value: " + lastValue);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{"IBM", 75.6f + i, 100});
        }
        Thread.sleep(4000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(1000), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"WSO2", 100.4f, 150});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 200.4f, 110});

        inputHandler.send(new Object[]{"IBM", 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 400.4f, 300});

        inputHandler.send(new Object[]{"IBM", 500.6f, 120});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 600.6f, 400});

        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();

        Thread.sleep(5000);
        inputHandler.send(new Object[]{"IBM", 700.6f, 230});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 800.6f, 125});

        inputHandler.send(new Object[]{"IBM", 900.6f, 370});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 1000.6f, 140});

        //shutdown Siddhi app
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 10));
        AssertJUnit.assertEquals(new Long(865), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest5() throws InterruptedException {
        log.info("Incremental persistence test 5 - external time window query");
        final int inputEventCount = 10;
        final int eventWindowSizeInSeconds = 2;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest5') " +
                "" +
                "define stream StockStream ( iij_timestamp long, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream[price>10]#window.externalTime(iij_timestamp, " + eventWindowSizeInSeconds + " sec) " +
                "select iij_timestamp, price, sum(volume) as totalVol " +
                "insert into OutStream";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    lastValue = (Long) inEvent.getData(2);
                    log.info("last value: " + lastValue);
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{(long) i, 75.6f + i, 100});
        }
        Thread.sleep(4000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(1000), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{(1L + inputEventCount), 100.4f, 150});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{(2L + inputEventCount), 200.4f, 110});

        inputHandler.send(new Object[]{(3L + inputEventCount), 300.4f, 100});
        //Thread.sleep(100);
        inputHandler.send(new Object[]{(4L + inputEventCount), 400.4f, 300});

        inputHandler.send(new Object[]{(5L + inputEventCount), 500.6f, 120});
        Thread.sleep(10);
        inputHandler.send(new Object[]{(6L + inputEventCount), 600.6f, 400});

        Thread.sleep(100);
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{(7L + inputEventCount), 700.6f, 230});
        Thread.sleep(10);
        inputHandler.send(new Object[]{(8L + inputEventCount), 800.6f, 125});

        inputHandler.send(new Object[]{(9L + inputEventCount), 900.6f, 370});
        Thread.sleep(10);
        inputHandler.send(new Object[]{(10L + inputEventCount), 1000.6f, 140});

        //shutdown Siddhi app
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 10));
        AssertJUnit.assertEquals(new Long(3045), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void incrementalPersistenceTest6() throws InterruptedException {
        log.info("Incremental persistence test 6 - in-memory table persistance test without primary key.");
        int inputEventCountPerCategory = 10;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String streams = "" +
                "@app:name('incrementalPersistenceTest6') " +
                "" +
                "define stream StockStream (symbol2 string, price float, volume long); " +
                "define stream CheckStockStream (symbol1 string); " +
                "define stream OutStream (symbol1 string, TB long); " +
                "define table StockTable (symbol2 string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from StockTable join CheckStockStream " +
                " on symbol2 == symbol1 " +
                "select symbol2 as symbol1, sum(StockTable.volume) as TB " +
                "group by symbol2 " +
                "insert all events into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        };

        try {
            siddhiAppRuntime.addCallback("query2", queryCallback);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

            siddhiAppRuntime.start();
            for (int i = 0; i < inputEventCountPerCategory; i++) {
                stockStream.send(new Object[]{"WSO2", 55.6f + i, 180L + i});
            }

            for (int i = 0; i < inputEventCountPerCategory; i++) {
                stockStream.send(new Object[]{"IBM", 55.6f + i, 100L + i});
            }

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(5000);

            siddhiAppRuntime.shutdown();

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

            siddhiAppRuntime.addCallback("query2", queryCallback);

            stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }
            siddhiAppRuntime.start();
            Thread.sleep(2000);

            stockStream.send(new Object[]{"WSO2", 100.6f, 180L});
            stockStream.send(new Object[]{"IBM", 100.6f, 100L});

            stockStream.send(new Object[]{"WSO2", 8.6f, 13L});
            stockStream.send(new Object[]{"IBM", 7.6f, 14L});

            checkStockStream.send(new Object[]{"IBM"});
            checkStockStream.send(new Object[]{"WSO2"});

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"IBM", 1159L},
                    new Object[]{"WSO2", 2038L}
            );

            Thread.sleep(1000);

            AssertJUnit.assertEquals("In events matched", true,
                    SiddhiTestHelper.isEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void incrementalPersistenceTest7() throws InterruptedException {
        log.info("Incremental persistence test 7 - in-memory table persistance test with primary key.");
        int inputEventCountPerCategory = 10;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String streams = "" +
                "@app:name('incrementalPersistenceTest7') " +
                "" +
                "define stream StockStream (symbol2 string, price float, volume long); " +
                "define stream CheckStockStream (symbol1 string); " +
                "define stream OutStream (symbol1 string, TB long); " +
                "@PrimaryKey('symbol2') " +
                "define table StockTable (symbol2 string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream join StockTable " +
                " on symbol1 == symbol2 " +
                "select symbol2 as symbol1, volume as TB " +
                "group by symbol2 " +
                "insert all events into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        };

        try {
            siddhiAppRuntime.addCallback("query2", queryCallback);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

            siddhiAppRuntime.start();
            for (int i = 0; i < inputEventCountPerCategory; i++) {
                stockStream.send(new Object[]{"WSO2-" + i, 55.6f, 180L + i});
            }

            for (int i = 0; i < inputEventCountPerCategory; i++) {
                stockStream.send(new Object[]{"IBM-" + i, 55.6f, 100L + i});
            }

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(5000);

            siddhiAppRuntime.shutdown();

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

            siddhiAppRuntime.addCallback("query2", queryCallback);

            stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed.", e);
            }
            siddhiAppRuntime.start();

            Thread.sleep(2000);

            stockStream.send(new Object[]{"WSO2-" + (inputEventCountPerCategory + 1), 100.6f, 180L});
            stockStream.send(new Object[]{"IBM-" + (inputEventCountPerCategory + 1), 100.6f, 100L});

            stockStream.send(new Object[]{"WSO2-" + (inputEventCountPerCategory + 2), 8.6f, 13L});
            stockStream.send(new Object[]{"IBM-" + (inputEventCountPerCategory + 2), 7.6f, 14L});

            checkStockStream.send(new Object[]{"IBM-5"});
            checkStockStream.send(new Object[]{"WSO2-5"});

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"IBM-5", 105L},
                    new Object[]{"WSO2-5", 185L}
            );

            Thread.sleep(1000);

            AssertJUnit.assertEquals("In events matched", true,
                    SiddhiTestHelper.isEventsMatch(inEventsList, expected));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void incrementalPersistenceTest8() throws InterruptedException {
        log.info("Incremental persistence test 8 - min-max counting.");
        final int inputEventCount = 10;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String streams = "" +
                "@app:name('incrementalPersistenceTest8') " +
                "define stream TempStream (roomNo long, temp long); " +
                "define stream MaxTempStream (roomNo long, maxTemp long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from TempStream#window.length(10) " +
                "select roomNo, max(temp) as maxTemp " +
                "insert into MaxTempStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        count++;
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                        lastValue = (Long) event.getData(1);
                    }
                    eventArrived = true;
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        };

        try {
            siddhiAppRuntime.addCallback("query1", queryCallback);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("TempStream");

            siddhiAppRuntime.start();
            for (int i = 0; i < inputEventCount; i++) {
                stockStream.send(new Object[]{i, 55L + i});
            }

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(5000);

            siddhiAppRuntime.shutdown();

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

            siddhiAppRuntime.addCallback("query1", queryCallback);

            stockStream = siddhiAppRuntime.getInputHandler("TempStream");
            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }
            siddhiAppRuntime.start();
            Thread.sleep(2000);

            stockStream.send(new Object[]{inputEventCount + 1, 1000L});

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(2000);

            stockStream.send(new Object[]{inputEventCount + 2, 20L});

            siddhiAppRuntime.persist();
            Thread.sleep(2000);

            stockStream.send(new Object[]{inputEventCount + 3, 30L});


            AssertJUnit.assertTrue(count <= (inputEventCount + 3));
            AssertJUnit.assertEquals(new Long(1000), lastValue);
            AssertJUnit.assertEquals(true, eventArrived);


        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void incrementalPersistenceTest9() throws InterruptedException {
        log.info("Incremental persistence test 9 - min-max counting with group-by.");
        final int inputEventCount = 10;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String streams = "" +
                "@app:name('incrementalPersistenceTest9') " +
                "define stream TempStream (roomNo long, temp long); " +
                "define stream MaxTempStream (roomNo long, maxTemp long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from TempStream#window.length(10) " +
                "select roomNo, max(temp) as maxTemp " +
                "group by roomNo " +
                "insert into MaxTempStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        count++;
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                        lastValue = (Long) event.getData(1);
                    }
                    eventArrived = true;
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        };

        try {
            siddhiAppRuntime.addCallback("query1", queryCallback);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("TempStream");

            siddhiAppRuntime.start();
            for (int i = 0; i < inputEventCount; i++) {
                stockStream.send(new Object[]{i, 55L + i});
            }

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(5000);

            siddhiAppRuntime.shutdown();

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

            siddhiAppRuntime.addCallback("query1", queryCallback);

            stockStream = siddhiAppRuntime.getInputHandler("TempStream");
            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }
            siddhiAppRuntime.start();
            Thread.sleep(2000);

            stockStream.send(new Object[]{inputEventCount + 1, 1000L});

            //persisting
            siddhiAppRuntime.persist();
            Thread.sleep(2000);

            stockStream.send(new Object[]{inputEventCount + 2, 20L});

            siddhiAppRuntime.persist();
            Thread.sleep(2000);

            stockStream.send(new Object[]{4, 50L});

            //AssertJUnit.assertTrue(count <= (inputEventCount + 3));
            AssertJUnit.assertEquals(new Long(59), lastValue);
            AssertJUnit.assertEquals(true, eventArrived);


        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void incrementalPersistenceTest10() throws InterruptedException {
        log.info("Incremental persistence test 10 - partitioned sum with group-by on length windows.");
        final int inputEventCount = 10;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "@app:name('incrementalPersistenceTest10') "
                + "define stream cseEventStreamOne (symbol string, price float,volume int);"
                + "partition with (price>=100 as 'large' or price<100 as 'small' of cseEventStreamOne) " +
                "begin @info(name " +
                "= 'query1') from cseEventStreamOne#window.length(4) select symbol,sum(price) as price " +
                "group by symbol insert into " +
                "OutStockStream ;  end ";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);

                eventArrived = true;
                if (events != null) {
                    for (Event event : events) {
                        count++;
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                        lastValue = ((Double) event.getData(1)).longValue();
                    }
                }
            }
        };

        siddhiAppRuntime.addCallback("OutStockStream", streamCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStreamOne");
        siddhiAppRuntime.start();

        for (int i = 0; i < inputEventCount; i++) {
            inputHandler.send(new Object[]{"IBM", 95f + i, 100});
            Thread.sleep(500);
            siddhiAppRuntime.persist();
        }

        siddhiAppRuntime.persist();
        inputHandler.send(new Object[]{"IBM", 205f, 100});
        Thread.sleep(2000);

        siddhiAppRuntime.shutdown();

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //siddhiAppRuntime.addCallback("query1", queryCallback);
        siddhiAppRuntime.addCallback("OutStockStream", streamCallback);

        inputHandler = siddhiAppRuntime.getInputHandler("cseEventStreamOne");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed", e);
        }
        siddhiAppRuntime.start();
        Thread.sleep(2000);

        inputHandler.send(new Object[]{"IBM", 105f, 100});

        Thread.sleep(1000);

        AssertJUnit.assertEquals(new Long(414), lastValue);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void incrementalPersistenceTest11() throws InterruptedException {
        log.info("Incremental persistence test 11");
        AtomicInteger count = new AtomicInteger();
        AtomicLong ibmCount = new AtomicLong();
        AtomicLong wso2Count = new AtomicLong();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "@app:name('incrementalPersistenceTest11') " +
                "define stream StockQuote (symbol string, price float, volume int);" +
                "partition with (symbol of StockQuote) " +
                "begin " +
                "@info(name = 'query1') " +
                "from StockQuote#window.length(4)  " +
                "select symbol, count(price) as price " +
                "group by symbol insert into " +
                "OutStockStream ;  end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                AssertJUnit.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);

                for (Event event : events) {
                    if ("IBM".equals(event.getData(0))) {
                        ibmCount.set((Long) event.getData(1));
                    } else {
                        wso2Count.set((Long) event.getData(1));
                    }
                }

                eventArrived = true;
            }
        };
        siddhiAppRuntime.addCallback("OutStockStream", streamCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockQuote");
        siddhiAppRuntime.start();

        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"IBM", 700f, 100}));
        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 60f, 50}));
        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 50f, 60}));
        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 40f, 60}));
        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 30f, 60}));

        siddhiAppRuntime.persist();

        Thread.sleep(2000);

        siddhiAppRuntime.shutdown();

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutStockStream", streamCallback);

        inputHandler = siddhiAppRuntime.getInputHandler("StockQuote");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

        siddhiAppRuntime.start();
        Thread.sleep(2000);

        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"IBM", 800f, 100}));
        inputHandler.send(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 20f, 60}));

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(7, count.get());
        AssertJUnit.assertEquals(2, ibmCount.get());
        AssertJUnit.assertEquals(4, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void incrementalPersistenceTest12() throws InterruptedException {
        log.info("Incremental file persistence test 12 - length window query with max attribute aggregator");
        final int eventWindowSize = 5;

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore(storageFilePath));

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest12') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream#window.length(" + eventWindowSize + ") " +
                "select symbol, price, max(volume) as maxVol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    AssertJUnit.assertTrue("IBM".equals(inEvent.getData(0)) ||
                            "WSO2".equals(inEvent.getData(0)));
                    lastValue = new Long((Integer) inEvent.getData(2));
                }
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"IBM", 75.6f, 500});
        inputHandler.send(new Object[]{"IBM", 75.6f, 200});
        inputHandler.send(new Object[]{"IBM", 75.6f, 300});
        inputHandler.send(new Object[]{"IBM", 75.6f, 250});
        inputHandler.send(new Object[]{"IBM", 75.6f, 150});

        Thread.sleep(100);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(new Long(500L), lastValue);

        //persisting
        siddhiAppRuntime.persist();
        Thread.sleep(5000);

        //persisting for the second time to store the inc-snapshot
        siddhiAppRuntime.persist();
        Thread.sleep(100);

        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);
        inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        siddhiAppRuntime.start();
        Thread.sleep(5000);

        inputHandler.send(new Object[]{"IBM", 100.4f, 280});
        AssertJUnit.assertEquals((Long) 300L, lastValue);

        inputHandler.send(new Object[]{"WSO2", 200.4f, 150});
        AssertJUnit.assertEquals((Long) 300L, lastValue);

        inputHandler.send(new Object[]{"IBM", 300.4f, 200});
        AssertJUnit.assertEquals((Long) 280L, lastValue);

        inputHandler.send(new Object[]{"WSO2", 400.4f, 270});
        AssertJUnit.assertEquals((Long) 280L, lastValue);

        inputHandler.send(new Object[]{"WSO2", 400.4f, 280});
        AssertJUnit.assertEquals((Long) 280L, lastValue);

        //shutdown Siddhi app
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertEquals(true, eventArrived);
    }
}

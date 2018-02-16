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

package org.wso2.siddhi.core.managment;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IncrementalPersistenceTestCase {
    private static final Logger log = Logger.getLogger(IncrementalPersistenceTestCase.class);
    private int count;
    private boolean eventArrived;
    private Long lastValue;
    private AtomicInteger inEventCount = new AtomicInteger(0);
    private int removeEventCount;
    private List<Object[]> inEventsList;
    private AtomicInteger count2 = new AtomicInteger(0);

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
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
        siddhiAppRuntime.start();

        Thread.sleep(500);
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

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
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
        siddhiAppRuntime.start();

        Thread.sleep(500);
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

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
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

        String siddhiApp = "" +
                "@app:name('incrementalPersistenceTest3') " +
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
        siddhiAppRuntime.start();

        Thread.sleep(500);
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

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
    public void incrementalPersistenceTest4() throws InterruptedException {
        log.info("Incremental persistence test 4 - time batch window query");
        final int inputEventCount = 10;
        final int eventWindowSizeInSeconds = 2;
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
        siddhiAppRuntime.start();

        Thread.sleep(500);
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

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
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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

        inputHandler.send(new Object[]{(3L  + inputEventCount), 300.4f, 100});
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
        siddhiAppRuntime.start();

        Thread.sleep(500);
        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

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

        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
            siddhiAppRuntime.start();

            Thread.sleep(1000);

            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }

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

        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
            siddhiAppRuntime.start();

            Thread.sleep(1000);

            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }

            Thread.sleep(2000);

            stockStream.send(new Object[]{"WSO2-" + (inputEventCountPerCategory + 1), 100.6f, 180L});
            stockStream.send(new Object[]{"IBM-"  + (inputEventCountPerCategory + 1), 100.6f, 100L});

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

        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
            siddhiAppRuntime.start();

            Thread.sleep(1000);

            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }

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

        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
            siddhiAppRuntime.start();

            Thread.sleep(1000);

            //loading
            try {
                siddhiAppRuntime.restoreLastRevision();
            } catch (CannotRestoreSiddhiAppStateException e) {
                Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
            }

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

        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

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
        siddhiAppRuntime.start();

        Thread.sleep(1000);

        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }

        Thread.sleep(2000);

        inputHandler.send(new Object[]{"IBM", 105f, 100});

        Thread.sleep(1000);

        AssertJUnit.assertEquals(new Long(515), lastValue);

        siddhiAppRuntime.shutdown();
    }
}

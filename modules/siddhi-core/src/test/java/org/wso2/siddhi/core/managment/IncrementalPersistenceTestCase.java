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
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.persistence.IncrementalFileSystemPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

public class IncrementalPersistenceTestCase {
    private static final Logger log = Logger.getLogger(IncrementalPersistenceTestCase.class);
    private int count;
    private boolean eventArrived;
    private Long lastValue;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        lastValue = 0L;
    }

    @Test
    public void persistenceTest1() throws InterruptedException {
        log.info("Incremental persistence test 1 - length window query");
        final int inputEventCount = 10;
        final int eventWindowSize = 4;
        //PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        PersistenceStore persistenceStore = new org.wso2.siddhi.core.util.persistence.FileSystemPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setIncrementalPersistenceStore(new IncrementalFileSystemPersistenceStore());

        String siddhiApp = "" +
                "@app:name('Test') " +
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
        inputHandler.send(new Object[]{"WSO2", 400.4f, 100});
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

        inputHandler.send(new Object[]{"IBM", 500.6f, 100});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"WSO2", 600.6f, 100});

        //shutdown Siddhi app
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(count <= (inputEventCount + 6));
        AssertJUnit.assertEquals(new Long(400), lastValue);
        AssertJUnit.assertEquals(true, eventArrived);
    }
}

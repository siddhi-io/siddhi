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

package io.siddhi.core.managment;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogTestCase {
    private static final Logger log = LogManager.getLogger(LogTestCase.class);
    private int count;
    private boolean eventArrived;
    private long firstValue;
    private long lastValue;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        firstValue = 0;
        lastValue = 0;
    }

    @Test
    public void persistenceTest1() throws InterruptedException {
        log.info("persistence test 1 - window query");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream StockStream ( symbol string, price float, volume int );" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream" +
                "   #log()" +
                "   #log('test message')" +
                "   #log(false)" +
                "   #log(true)" +
                "   #log('test message',false)" +
                "   #log('test message',true)" +
                "   #log('error','test message')" +
                "   #log('error','test message',false)" +
                "   #log('warn','test message',true)" +
                "select * " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
            }
        };

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Event[]{
                new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100}),
                new Event(System.currentTimeMillis(), new Object[]{"GOOG", 70.6f, 100})});
        Thread.sleep(100);

        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(true, eventArrived);
    }
}

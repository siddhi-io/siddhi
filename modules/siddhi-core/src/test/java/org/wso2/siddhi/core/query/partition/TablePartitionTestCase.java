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

package org.wso2.siddhi.core.query.partition;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;


public class TablePartitionTestCase {
    private static final Logger log = Logger.getLogger(TablePartitionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int stockStreamEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
        stockStreamEventCount = 0;
    }

    @Test
    public void testPartitionQuery() throws InterruptedException {
        log.info("Table Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('PartitionTest') " +
                "" +
                "define stream streamA (symbol string, price int);" +
                "define table tableA (symbol string, price int);" +
                "partition with (symbol of streamA) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from streamA " +
                "   select symbol, price " +
                "   insert into tableA;  " +
                "end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("streamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700});
        inputHandler.send(new Object[]{"WSO2", 60});
        inputHandler.send(new Object[]{"WSO2", 60});
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(0, count.get());
    }

    @Test
    public void testPartitionQuery1() throws InterruptedException {
        log.info("Table Partition test 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "define stream streamB (symbol string);" +
                "define table tableA (symbol string, price int);" +
                "partition with (symbol of streamA, symbol of streamB) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from streamA " +
                "   select symbol, price " +
                "   insert into tableA;  " +
                "" +
                "   @info(name = 'query2') " +
                "   from streamB[(symbol==tableA.symbol) in tableA] " +
                "   select symbol " +
                "   insert into outputStream;  " +
                "end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                }
                eventArrived = true;
            }
        });

        InputHandler streamAInputHandler = siddhiAppRuntime.getInputHandler("streamA");
        InputHandler streamBInputHandler = siddhiAppRuntime.getInputHandler("streamB");
        siddhiAppRuntime.start();
        streamAInputHandler.send(new Object[]{"IBM", 700});
        streamAInputHandler.send(new Object[]{"WSO2", 60});
        streamAInputHandler.send(new Object[]{"WSO2", 60});
        streamBInputHandler.send(new Object[]{"WSO2"});
        streamBInputHandler.send(new Object[]{"FB"});
        streamBInputHandler.send(new Object[]{"IBM"});
        SiddhiTestHelper.waitForEvents(100, 2, count, 60000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertEquals(true, eventArrived);
    }

    @Test
    public void testPartitionQuery2() throws InterruptedException {
        log.info("Table Partition test 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "define stream streamB (symbol string);" +
                "define table tableA (symbol string, price int);" +
                "partition with (symbol of streamA, symbol of streamB) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from streamA " +
                "   select symbol, price " +
                "   insert into tableA;  " +
                "" +
                "end ;" +
                "" +
                "@info(name = 'query2') " +
                "from streamB[(symbol==tableA.symbol) in tableA] " +
                "select symbol " +
                "insert into outputStream;  " +
                "";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                }
                eventArrived = true;
            }
        });

        InputHandler streamAInputHandler = siddhiAppRuntime.getInputHandler("streamA");
        InputHandler streamBInputHandler = siddhiAppRuntime.getInputHandler("streamB");
        siddhiAppRuntime.start();
        streamAInputHandler.send(new Object[]{"IBM", 700});
        streamAInputHandler.send(new Object[]{"WSO2", 60});
        streamAInputHandler.send(new Object[]{"WSO2", 60});
        streamBInputHandler.send(new Object[]{"WSO2"});
        streamBInputHandler.send(new Object[]{"FB"});
        streamBInputHandler.send(new Object[]{"IBM"});
        SiddhiTestHelper.waitForEvents(100, 2, count, 60000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertEquals(true, eventArrived);
    }


}

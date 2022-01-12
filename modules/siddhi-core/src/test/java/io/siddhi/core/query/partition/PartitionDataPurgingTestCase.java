/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.query.partition;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PartitionDataPurgingTestCase {
    private static final Logger log = LogManager.getLogger(PartitionDataPurgingTestCase.class);
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
    public void testPartitionPurgQuery1() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('PartitionTest') " +
                "" +
                "define stream streamA (symbol string, price int);" +
                "" +
                "" +
                "@purge(enable='true', interval='1 sec', idle.period='1 sec') " +
                "partition with (symbol of streamA) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from streamA#window.length(3) " +
                "   select symbol, avg(price) as total " +
                "   insert into StockQuote ;  " +
                "end ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                AssertJUnit.assertTrue("IBM".equals(events[0].getData(0)) ||
                        "WSO2".equals(events[0].getData(0)));
                for (Event event : events) {
                    int eventCount = count.incrementAndGet();
                    switch (eventCount) {
                        case 1:
                            Assert.assertEquals(event.getData(1), 100.0);
                            break;
                        case 2:
                            Assert.assertEquals(event.getData(1), 100.0);
                            break;
                        case 3:
                            Assert.assertEquals(event.getData(1), 200.0);
                            break;
                        case 4:
                            Assert.assertEquals(event.getData(1), 40.0);
                            break;
                        case 5:
                            Assert.assertEquals(event.getData(1), 25.0);
                            break;
                        case 6:
                            Assert.assertEquals(event.getData(1), 20.0);
                            break;
                        case 7:
                            Assert.assertEquals(event.getData(1), 100.0);
                            break;
                        case 8:
                            Assert.assertEquals(event.getData(1), 10.0);
                            break;
                    }

                }
                eventArrived = true;
            }
        };
        siddhiAppRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("streamA");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"IBM", 400});
        inputHandler.send(new Object[]{"WSO2", 40});
        inputHandler.send(new Object[]{"WSO2", 10});
        inputHandler.send(new Object[]{"WSO2", 10});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 10});

        SiddhiTestHelper.waitForEvents(100, 8, count, 60000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(8, count.get());
        siddhiAppRuntime.shutdown();
    }

}

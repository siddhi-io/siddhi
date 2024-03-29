/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.window;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CronWindowTestCase {
    private static final Logger log = LogManager.getLogger(CronWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testCronWindow1() throws InterruptedException {
        log.info("Testing cron window for current events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) cron('*/5 * * * * ?'); ";

        String query = "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') from cseEventWindow " +
                "select symbol,price,volume " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if (event.isExpired()) {
                        removeEventCount++;
                    } else {
                        inEventCount++;
                    }
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(7000);
        inputHandler.send(new Object[]{"IBM1", 700f, 0});
        inputHandler.send(new Object[]{"WSO22", 60.5f, 1});
        Thread.sleep(7000);
        inputHandler.send(new Object[]{"IBM43", 700f, 0});
        inputHandler.send(new Object[]{"WSO4343", 60.5f, 1});
        Thread.sleep(7000);
        AssertJUnit.assertEquals(6, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }


    @Test
    public void testCronWindow2() throws InterruptedException {
        log.info("Testing cron window for expired events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) cron('*/5 * * * * ?') output " +
                "expired events; ";

        String query = "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') from cseEventWindow " +
                "select symbol,price,volume " +
                "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    removeEventCount++;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(7000);
        inputHandler.send(new Object[]{"IBM1", 700f, 0});
        inputHandler.send(new Object[]{"WSO22", 60.5f, 1});
        Thread.sleep(7000);
        inputHandler.send(new Object[]{"IBM43", 700f, 0});
        inputHandler.send(new Object[]{"WSO4343", 60.5f, 1});
        Thread.sleep(7000);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }


}

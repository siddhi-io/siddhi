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

package io.siddhi.core.query.window;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExternalTimeWindowTestCase {

    private static final Logger log = LogManager.getLogger(TimeWindowTestCase.class);
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
    public void externalTimeWindowTest1() throws InterruptedException {

        log.info("externalTimeWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.externalTime(timestamp,5 sec) " +
                "select timestamp, ip  " +
                "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804342L, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814341L, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814345L, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824341L, "192.10.1.7"});

        Thread.sleep(1000);

        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("In Events ", 5, inEventCount);
        AssertJUnit.assertEquals("Remove Events ", 4, removeEventCount);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void externalTimeWindowTest2() throws InterruptedException {

        log.info("externalTimeWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string) ;";
        String query = "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.externalTime(timestamp) " +
                "select timestamp, ip  " +
                "insert all events into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1366335804341L, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804342L, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814341L, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814345L, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824341L, "192.10.1.7"});

        Thread.sleep(1000);

        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("In Events ", 5, inEventCount);
        AssertJUnit.assertEquals("Remove Events ", 4, removeEventCount);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void externalTimeWindowTest3() throws InterruptedException {
        log.info("externalTimeWindow test3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream LoginEvents (timestamp int, ip string) ;";
        String query = "@info(name = 'query1') " +
                "from LoginEvents#window.externalTime(timestamp,5 sec) " +
                "select timestamp, ip  " +
                "insert all events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{1366335804, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335802, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824, "192.10.1.7"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("In Events ", 5, inEventCount);
        AssertJUnit.assertEquals("Remove Events ", 4, removeEventCount);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void externalTimeWindowTest4() throws InterruptedException {
        log.info("externalTimeWindow test3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream LoginEvents (timestamp int, ip string) ;";
        String query = "" +
                "@info(name = 'query1') " +
                "from LoginEvents#window.externalTime('timestamp',5 sec) " +
                "select timestamp, ip  " +
                "insert all events into uniqueIps ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{1366335804, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335802, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824, "192.10.1.7"});
        Thread.sleep(1000);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("In Events ", 5, inEventCount);
        AssertJUnit.assertEquals("Remove Events ", 4, removeEventCount);
        siddhiAppRuntime.shutdown();
    }
}

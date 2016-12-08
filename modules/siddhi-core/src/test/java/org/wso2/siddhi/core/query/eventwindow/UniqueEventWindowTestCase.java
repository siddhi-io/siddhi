/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.core.query.eventwindow;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class UniqueEventWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueEventWindowTestCase.class);
    private int count;
    private long value;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        value = 0;
        eventArrived = false;
    }

    @Test
    public void testUniqueWindow1() throws InterruptedException {
        log.info("UniqueWindow Test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timeStamp long, ip inputmapper); " +
                "define window LoginWindow (timeStamp long, ip inputmapper) unique(ip) output all events; ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from LoginWindow " +
                "select count(ip) as ipCount, ip " +
                "insert into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(0);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.4"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{System.currentTimeMillis(), "192.10.1.5"});

        Thread.sleep(1000);

        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("Event max value", 3, value);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueWindow2() throws InterruptedException {
        log.info("UniqueWindow Test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (id String , timeStamp long, ip inputmapper); " +
                "define window LoginWindow (id String , timeStamp long, ip inputmapper) unique(ip) output all events; ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from LoginWindow " +
                "select id, count(ip) as ipCount, ip " +
                "insert into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"A1", System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{"A2", System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{"A3", System.currentTimeMillis(), "192.10.1.4"});
        inputHandler.send(new Object[]{"A4", System.currentTimeMillis(), "192.10.1.3"});
        inputHandler.send(new Object[]{"A5", System.currentTimeMillis(), "192.10.1.5"});

        Thread.sleep(1000);

        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("Event max value", 3, value);

        executionPlanRuntime.shutdown();
    }

}

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

public class SortEventWindowTestCase {
    private static final Logger log = Logger.getLogger(SortEventWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testSortWindow1() throws InterruptedException {
        log.info("SortWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) sort(2,volume, 'asc'); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 55.6f, 100l});
        inputHandler.send(new Object[]{"IBM", 75.6f, 300l});
        inputHandler.send(new Object[]{"WSO2", 57.6f, 200l});
        inputHandler.send(new Object[]{"WSO2", 55.6f, 20l});
        inputHandler.send(new Object[]{"WSO2", 57.6f, 40l});
        Thread.sleep(1000);
        Assert.assertEquals(5, inEventCount);
        Assert.assertEquals(3, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testSortWindow2() throws InterruptedException {
        log.info("SortWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "define window cseEventWindow (symbol string, price float, volume long) sort(2,volume, 'asc', price, 'desc'); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventWindow " +
                "select volume " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 50, 100l});
        inputHandler.send(new Object[]{"IBM", 20, 100l});
        inputHandler.send(new Object[]{"WSO2", 40, 50l});
        inputHandler.send(new Object[]{"WSO2", 100, 20l});
        inputHandler.send(new Object[]{"WSO2", 50, 50l});
        Thread.sleep(1000);
        Assert.assertEquals(5, inEventCount);
        Assert.assertEquals(3, removeEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }
}

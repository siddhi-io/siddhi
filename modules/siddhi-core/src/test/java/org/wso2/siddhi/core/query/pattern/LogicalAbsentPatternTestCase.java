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

package org.wso2.siddhi.core.query.pattern;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class LogicalAbsentPatternTestCase {

    private static final Logger log = Logger.getLogger(LogicalAbsentPatternTestCase.class);
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
    public void testQueryAbsent1() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec and e3 with e1 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] and e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent2() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec and e3 with e1, e2 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] and e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 25.0f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent3() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec and e2 -> e3 with e2 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] and e2=Stream2[price>20] -> e3=Stream3[price>30] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream2.send(new Object[]{"IBM", 25.0f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent4() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec or e3 with e1 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec or e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent5() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec or e3 with e1 only");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec or e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent6() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec or e3 with e1, e2 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec or e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 25.0f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent7() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec or e3 with e1 and e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec or e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");


        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.0f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 25.0f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

//    @Test
//    public void testQueryAbsent8() throws InterruptedException {
//        log.info("Test the query not e1 within 1 sec -> not e2 and e3 -> e4 with e2 and e3");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String streams = "" +
//                "define stream Stream1 (symbol string, price float, volume int); " +
//                "define stream Stream2 (symbol string, price float, volume int); " +
//                "define stream Stream3 (symbol string, price float, volume int); " +
//                "define stream Stream4 (symbol string, price float, volume int); ";
//        String query = "" +
//                "@info(name = 'query1') " +
//                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] and e3=Stream3[price>30] -> " +
//                "e4=Stream4[price>40]" +
//                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
//                "insert into OutputStream ;";
//
//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
//
//        addCallback(executionPlanRuntime, "query1");
//
//        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
//        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
//        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
//        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");
//
//
//        executionPlanRuntime.start();
//
////        stream1.send(new Object[]{"WSO2", 15.0f, 100});
////        Thread.sleep(100);
//        stream2.send(new Object[]{"IBM", 25.0f, 100});
//        Thread.sleep(100);
//        stream3.send(new Object[]{"GOOGLE", 35.0f, 100});
//        Thread.sleep(100);
//        stream4.send(new Object[]{"ORACLE", 45.0f, 100});
//        Thread.sleep(1100);
//
//        Assert.assertEquals("Number of success events", 1, inEventCount);
//        Assert.assertEquals("Number of remove events", 0, removeEventCount);
//        Assert.assertTrue("Event arrived", eventArrived);
//
//        executionPlanRuntime.shutdown();
//    }

    private void addCallback(ExecutionPlanRuntime executionPlanRuntime, String queryName) {
        executionPlanRuntime.addCallback(queryName, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
    }
}

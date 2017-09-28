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

package org.wso2.siddhi.extension.regex;

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
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class FindFunctionExtensionTestCase {
    static final Logger log = Logger.getLogger(FindFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testFindFunctionExtensionTestCase() throws InterruptedException {
        log.info("FindFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count == 2) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count == 3) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count == 4) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"21 products are produced by WSO2 currently", 60.5f, "\\d\\d(.*)WSO2"});
        inputHandler.send(new Object[]{"WSO2 is situated in trace and its a middleware company", 60.5f,
                                       "WSO2(.*)middleware(.*)"});
        inputHandler.send(new Object[]{"WSO2 is situated in trace and its a middleware company", 60.5f,
                                       "WSO2(.*)middleware"});
        inputHandler.send(new Object[]{"21 products are produced by WSO2 currently", 60.5f, "\\d(.*)WSO22"});
        Thread.sleep(100);
        Assert.assertEquals(4, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testFindStartFromIndexFunctionExtensionTestCase() throws InterruptedException {
        log.info("FindStartFromIndexFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string, "
                + "startingIndex int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol, startingIndex) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count == 2) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"21 products are produced within 10 years by WSO2 currently "
                                               + "by WSO2 employees", 60.5f, "\\d\\d(.*)WSO2", 30});
        inputHandler.send(new Object[]{"21 products are produced within 10 years by WSO2 currently "
                                               + "by WSO2 employees", 60.5f, "\\d\\d(.*)WSO2", 35});

        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test (expected = ExecutionPlanValidationException.class)
    public void testFindFunctionExtensionTestCase1() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with invalid number of arguments");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test (expected = ExecutionPlanValidationException.class)
    public void testFindFunctionExtensionTestCase2() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with invalid datatype");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test (expected = ExecutionPlanValidationException.class)
    public void testFindFunctionExtensionTestCase3() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with invalid datatype");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol int, price long, regex string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test (expected = ExecutionPlanValidationException.class)
    public void testFindFunctionExtensionTestCase4() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with invalid datatype");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string, "
                + "startingIndex string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol, startingIndex) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testFindFunctionExtensionTestCase5() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"21 products are produced by WSO2 currently", 60.5f, null});
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testFindFunctionExtensionTestCase6() throws InterruptedException {
        log.info("FindFunctionExtension TestCase with null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string, "
                + "startingIndex int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol, startingIndex) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    if (count == 2) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{null, 60.5f, "\\d\\d(.*)WSO2", 30});
        inputHandler.send(new Object[]{"21 products are produced within 10 years by WSO2 currently "
                                               + "by WSO2 employees", 60.5f, "\\d\\d(.*)WSO2", null});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testFindFunctionExtensionTestCase7() throws InterruptedException {
        log.info("FindStartFromIndexFunctionExtension TestCase invalid input value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, regex string, "
                + "startingIndex int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , regex:find(regex, symbol, startingIndex) as aboutWSO2 " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"21 products are produced within 10 years by WSO2 currently "
                                               + "by WSO2 employees", 60.5f, "\\d\\d(.*)WSO2", "WSO2"});
        executionPlanRuntime.shutdown();
    }
}

/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.time;

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

public class DateDifferenceFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(DateDifferenceFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void dateDifferenceFunctionExtension() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                eventArrived = true;
                for (int cnt = 0; cnt < inEvents.length; cnt++) {
                    count++;
                    log.info("Event : " + count + ",dateDifference : " + inEvents[cnt].getData(1) + "," +
                            "dateDifferenceInMilliseconds : " + inEvents[cnt].getData(2));

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014-10-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1412841224000L});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2013-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1384156424000L});
        Thread.sleep(1000);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateDifferenceFunctionExtensionTest2() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionFirstDateInvalidFormatTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:11:11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", "2015:11:11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateDifferenceFunctionExtensionTest3() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionSecondDateInvalidFormatTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014:11:9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2015:11:9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension4() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionInvalidParameterTypeInFirstArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 int,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension5() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionInvalidParameterTypeInSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 int," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension6() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionInvalidParameterTypeInThirdArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 int,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension7() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionInvalidParameterTypeInFourthArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 int," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test
    public void dateDifferenceFunctionExtension8() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionFirstArgumetNullTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, "yyyy-MM-dd HH:mm:ss",
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", null, "yyyy-MM-dd HH:mm:ss",
                "2016-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateDifferenceFunctionExtension9() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionThirdArgumetNullTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", null,
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", "2015-11-11 13:23:44", null,
                "2012-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateDifferenceFunctionExtension10() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionSecondArgumetNullTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2", event.getData(2).toString());
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                null, "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        inputHandler.send(new Object[]{"IBM", "2015-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                null, "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension11() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionInvalid parameter in second argument for length 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1,dateFormat2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension12() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidNoOfArguments");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test
    public void dateDifferenceFunctionExtension13() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseCastingDesiredFormat");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 string," +
                "unit string," +
                "timestampInMilliseconds1 string,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol ,time:dateDiff(timestampInMilliseconds1,dateFormat1) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",
                "2014-11-9 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, 1415519624000L});
        Thread.sleep(1000);
        Assert.assertEquals(1, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension14() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidParameterTypeFirstArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 int,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension15() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidParameterTypeSecondArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 int," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension16() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidParameterTypeSecondArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 int,dateFormat1 string,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension17() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidParameterTypeSecondArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 string,dateValue2 int," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateDifferenceFunctionExtension18() throws InterruptedException {

        log.info("DateDifferenceFunctionExtensionTestCaseInvalidParameterTypeThirdArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue1 string,dateFormat1 int,dateValue2 string," +
                "dateFormat2 string," +
                "timestampInMilliseconds1 long,timestampInMilliseconds2 long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:dateDiff(dateValue1,dateValue2,dateFormat1) as dateDifference," +
                "time:dateDiff(timestampInMilliseconds1,timestampInMilliseconds2) as dateDifferenceInMilliseconds " +
                "insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }
}






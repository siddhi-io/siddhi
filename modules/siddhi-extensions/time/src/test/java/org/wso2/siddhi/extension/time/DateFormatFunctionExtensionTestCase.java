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

public class DateFormatFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(DateFormatFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void dateFormatFunctionExtension() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

                eventArrived = true;
                for (int cnt = 0; cnt < inEvents.length; cnt++) {
                    count++;
                    log.info("Event : " + count + ",formattedDate : " + inEvents[cnt].getData(1) + "," +
                                     "formattedMillsDate : " + inEvents[cnt].getData(2));

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, "ss"});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, "ss"});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44.657", "yyyy-MM-dd HH:mm:ss.SSS", 1415692424000L,
                                       "yyyy-MM-dd"});
        Thread.sleep(100);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateFormatFunctionExtension2() throws InterruptedException {

        log.info("DateFormatFunctionExtensionInvalidFormatTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2014-11-11", event.getData(2));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2014-11-11", event.getData(2));
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L,
                                       "yyyy-MM-dd"});
        inputHandler.send(new Object[]{"IBM", "2014:12-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L,
                                       "yyyy-MM-dd"});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateFormatFunctionExtension3() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseInvalidParameterTypeInFirstArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue int,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateFormatFunctionExtension4() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseInvalidParameterTypeInSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat int);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateFormatFunctionExtension5() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseInvalidParameterTypeInThirdArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat int,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateFormatFunctionExtension6() throws InterruptedException {

        log.info("DateFormatFunctionExtensionInvalidNoOfArgumentTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateFormatFunctionExtension7() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseFirstArgumentNul");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("2014-11-11", event.getData(2));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("44", event.getData(2));
                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, "yyyy-MM-dd HH:mm:ss", 1415692424000L, "yyyy-MM-dd"});
        inputHandler.send(new Object[]{"IBM", null, "yyyy-MM-dd HH:mm:ss", 1415692424000L, "ss"});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateFormatFunctionExtension8() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseSecondArgumentNull");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, null});
        inputHandler.send(new Object[]{"IBM", "2015:11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, null});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void dateFormatFunctionExtension9() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseThirdArgumentNul");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds long,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("44", event.getData(2));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals("44",event.getData(2));
                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:11-11 13:23:44", null, 1415692424000L, "ss"});
        inputHandler.send(new Object[]{"IBM", "2014:11-11 13:23:44", null, 1415692424000L, "ss"});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test
    public void dateFormatFunctionExtension10() throws InterruptedException {

        log.info("DateFormatFunctionExtensionTestCaseForCastingDesiredFormat");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string," +
                "dateValue string,sourceFormat string,timestampInMilliseconds string,targetFormat string);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "time:dateFormat(dateValue,targetFormat,sourceFormat) as formattedDate," +
                "time:dateFormat(timestampInMilliseconds," +
                "targetFormat) as formattedUnixDate insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals("44", event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                    }
                    if (count == 2) {
                        Assert.assertEquals("44",event.getData(1));
                        Assert.assertEquals(null, event.getData(2));

                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, "ss"});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss", 1415692424000L, "ss"});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}

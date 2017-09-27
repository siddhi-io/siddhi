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

public class DateSubFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(DateSubFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void dateSubFunctionExtension() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted,time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for(int cnt=0;cnt<inEvents.length;cnt++){
                    count++;
                    log.info("Event : " + count + ",YEAR_SUBTRACTED : " + inEvents[cnt].getData(1) +"," +
                            "MONTH_SUBTRACTED : "+inEvents[cnt].getData(2) + "," +
                            "YEAR_SUBTRACTED_IN_MILLS : "+inEvents[cnt].getData(3));
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        inputHandler.send(new Object[]{"IBM", "2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        Thread.sleep(100);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension2() throws InterruptedException{

        log.info("TestCaseForDateSubFunctionExtensionInvalidArgumentsIntime:dateSubFunction");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds " +
                "long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr) as yearSubtracted,time:dateSub(" +
                "dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') " +
                "as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension3() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseInvalidParameterTypeInSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds " +
                "long,expr String);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted," +
                "time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as " +
                "yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension4() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseForInvalidParameterTypeInFourthArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat int,timestampInMilliseconds long," +
                "expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted," +
                "time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as " +
                "yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension5() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseForInvalidParameterTypeInFirstArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue int,dateFormat string,timestampInMilliseconds " +
                "long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted," +
                "time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as " +
                "yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition
                + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test
    public void dateSubFunctionExtension6() throws InterruptedException {

        log.info("DateSubFunctionExtensionInvalidFormatTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds " +
                "long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted," +
                "time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as " +
                "yearSubtractedUnix "+
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
                        Assert.assertEquals(null, event.getData(2));
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null,event.getData(2));
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }

                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:11:11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        inputHandler.send(new Object[]{"IBM", "2014,11,11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test
    public void dateSubFunctionExtension7() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseFirstArgumentNull");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted,time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

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
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null,event.getData(2));
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM",null, "yyyy-MM-dd HH:mm:ss",1415692424000L,2});
        inputHandler.send(new Object[]{"IBM",null, "ss",1415692424000L,2});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test
    public void dateSubFunctionExtension8() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseFourthArgumentNull");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted,time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

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
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null,event.getData(2));
                        Assert.assertEquals("1352620424000", event.getData(3));
                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM","2014-11-11 13:23:44", null,1415692424000L,2});
        inputHandler.send(new Object[]{"IBM","2015-11-11 13:23:44", null,1415692424000L,2});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test
    public void dateSubFunctionExtension9() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseSecondArgumentNull");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds long,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,'year',dateFormat) as yearSubtracted,time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

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
                        Assert.assertEquals(null, event.getData(3));
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null,event.getData(2));
                        Assert.assertEquals(null, event.getData(3));
                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM","2014-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,null});
        inputHandler.send(new Object[]{"IBM","2011-11-11 13:23:44", "yyyy-MM-dd HH:mm:ss",1415692424000L,null});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension10() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseUnitValueConstant");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long,expr int,unit string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,unit) as yearSubtracted,time:dateSub(dateValue,expr," +
                "'month',dateFormat) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension11() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long,unit int ,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr,unit) as yearSubtracted,time:dateSub(dateValue,expr," +
                "unit) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension12() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseInvalidParameterFormatSecondArgumentLengthThree");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long,unit int ,expr string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr," +
                "unit) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension13() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseInvalidParameterFormatThirdArgumentLengthThree");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long,unit int ,expr int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr," +
                "unit) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
    @Test(expected = ExecutionPlanValidationException.class)
    public void dateSubFunctionExtension14() throws InterruptedException {

        log.info("DateSubFunctionExtensionTestCaseInvalidParameterFormatFirstArgumentLengthThree");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue int,dateFormat string,timestampInMilliseconds" +
                " long,unit int ,expr string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream select symbol ,time:dateSub(dateValue,expr," +
                "unit) as monthSubtracted,time:dateSub(timestampInMilliseconds,expr,'year') as yearSubtractedUnix "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }
}

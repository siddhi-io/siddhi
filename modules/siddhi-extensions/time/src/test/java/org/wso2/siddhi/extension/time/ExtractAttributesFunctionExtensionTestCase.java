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

public class ExtractAttributesFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(ExtractAttributesFunctionExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void extractAttributesFunctionExtension() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (int cnt = 0; cnt < inEvents.length; cnt++) {
                    count++;
                    log.info("Event : " + count + ",YEAR : " + inEvents[cnt].getData(1) + "," +
                            "MONTH : " + inEvents[cnt].getData(2) + ",HOUR : " + inEvents[cnt].getData(3));

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014-3-11 02:23:44", "yyyy-MM-dd hh:mm:ss", 1394484824000L});
        inputHandler.send(new Object[]{"IBM", "2014-3-11 02:23:44", "yyyy-MM-dd hh:mm:ss", 1394484824000L});
        inputHandler.send(new Object[]{"IBM", "2014-3-11 22:23:44", "yyyy-MM-dd hh:mm:ss", 1394556804000L});
        Thread.sleep(100);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension2() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionInvalidFormatTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds " +
                "long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
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
                        Assert.assertEquals("2", event.getData(3).toString());


                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                        Assert.assertEquals("2", event.getData(3).toString());

                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:3-11 02:23:44", "yyyy-MM-dd hh:mm:ss", 1394484824000L});
        inputHandler.send(new Object[]{"IBM", "2015:3-11 02:23:44", "yyyy-MM-dd hh:mm:ss", 1394484824000L});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension3() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionInvalidParameterTypeInSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue int,dateFormat string,timestampInMilliseconds " +
                "long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension4() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionInvalidParameterTypeInThirdArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat int,timestampInMilliseconds " +
                "long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension5() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionInvalidParameterTypeInFirstArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds " +
                "int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension6() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionInvalidNoOfArgumentTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract('HOUR') as HOUR " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                query);
        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension7() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionSecondArgumentNullTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
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
                        Assert.assertEquals("2", event.getData(3).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                        Assert.assertEquals("2", event.getData(3).toString());

                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", null, "yyyy-MM-dd hh:mm:ss", 1394484824000L});
        inputHandler.send(new Object[]{"IBM", null, "ss", 1394484824000L});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension8() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionThirdArgumentNullTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('YEAR',dateValue,dateFormat) as YEAR,time:extract('MONTH',dateValue," +
                "dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR " +
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
                        Assert.assertEquals("2", event.getData(3).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(null, event.getData(1));
                        Assert.assertEquals(null, event.getData(2));
                        Assert.assertEquals("2", event.getData(3).toString());

                    }

                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2014:3-11 02:23:44", null, 1394484824000L});
        inputHandler.send(new Object[]{"IBM", "2012:3-11 02:23:44", null, 1394484824000L});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }


}

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
import org.apache.commons.lang3.LocaleUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.time.util.TimeExtensionConstants;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

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

        log.info("ExtractAttributesFunctionExtensionTestCase: " +
                "<int>  time: extract (<string> unit ,<string>  dateValue, <string> dataFormat)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string," +
                "timestampInMilliseconds long);";
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
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
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
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
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
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
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
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
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

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension9() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionTestCaseInvalidParameterFirstArgumentLengthThree");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,unit long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(unit,dateValue,dateFormat) as YEAR insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension10() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionTestCaseInvalidParameterSecondArgumentLengthTwo");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue int,dateFormat string,unit string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(unit,dateValue) as YEAR insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension11() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionTestCaseInvalidParameterLengthTwoDefaultDateFalseSecondArgument");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,timestampInMilliseconds long,unit int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(timestampInMilliseconds,unit) as YEAR insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void extractAttributesFunctionExtension12() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionUnitValueConstantTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,timestampInMilliseconds string,unit string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(timestampInMilliseconds,unit) as YEAR insert into outputStream;");
        siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
    }

    @Test
    public void extractAttributesFunctionExtension13() throws InterruptedException {

        log.info("ExtractAttributesFunctionExtensionProcessedCalenderTestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string,timestampInMilliseconds" +
                " long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('SECOND',dateValue,dateFormat) as SECOND,time:extract('MONTH'," +
                "dateValue,dateFormat) as MONTH,time:extract(timestampInMilliseconds,'HOUR') as HOUR," +
                "time:extract(timestampInMilliseconds,'MINUTE') as MINUTE," +
                "time:extract(timestampInMilliseconds,'HOUR_OF_DAY') as HOUR_OF_DAY " +
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
                        Assert.assertEquals(44, event.getData(1));
                        Assert.assertEquals(3, event.getData(2));
                        Assert.assertEquals("2", event.getData(3).toString());
                    }
                    if (count == 2) {
                        Assert.assertEquals(44, event.getData(1));
                        Assert.assertEquals(3, event.getData(2));
                        Assert.assertEquals("2", event.getData(3).toString());
                    }
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
    public void extractAttributesFunctionExtension14() throws InterruptedException, ParseException {

        log.info("ExtractAttributesFunctionExtensionTestCase2: " +
                "<int>  time: extract (<string> unit ,<string>  dateValue, <string> dataFormat, <string> locale)");
        SiddhiManager siddhiManager = new SiddhiManager();
        Calendar calendarEN = Calendar.getInstance(LocaleUtils.toLocale("en_US"));
        Calendar calendarFR = Calendar.getInstance(LocaleUtils.toLocale("fr_FR"));
        FastDateFormat userSpecificFormat;
        userSpecificFormat = FastDateFormat.getInstance("yyyy-MM-dd");
        Date userSpecifiedDate = userSpecificFormat.parse("2017-10-8");
        calendarEN.setTime(userSpecifiedDate);
        calendarFR.setTime(userSpecifiedDate);
        final Integer valueEN =  calendarEN.get(Calendar.WEEK_OF_YEAR);
        final Integer valueFR =  calendarFR.get(Calendar.WEEK_OF_YEAR);

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string," +
                "timestampInMilliseconds long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('WEEK',dateValue,dateFormat, 'en_US') as WEEK "+
                "insert into outputStream;");
        String query2 = ("@info(name = 'query2') " +
                "from inputStream " +
                "select symbol , time:extract('WEEK',dateValue,dateFormat, 'fr_FR') as WEEK "+
                "insert into outputStream2;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition
                + query + query2);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueEN, inEvent.getData(1));
                }
            }
        });

        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueFR, inEvent.getData(1));
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2017-10-8", "yyyy-MM-dd",1507401000000L});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension15() throws InterruptedException, ParseException {

        log.info("extractAttributesFunctionExtension15: " +
                "<int>  time: extract (<long> timestampInMilliseconds ,<string>  unit, <string> locale)");
        SiddhiManager siddhiManager = new SiddhiManager();
        Calendar calendarEN = Calendar.getInstance(LocaleUtils.toLocale("en_US"));
        Calendar calendarFR = Calendar.getInstance(LocaleUtils.toLocale("fr_FR"));
        calendarEN.setTimeInMillis(1507401000000L);
        calendarFR.setTimeInMillis(1507401000000L);
        final Integer valueEN =  calendarEN.get(Calendar.WEEK_OF_YEAR);
        final Integer valueFR =  calendarFR.get(Calendar.WEEK_OF_YEAR);

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string," +
                "timestampInMilliseconds long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(timestampInMilliseconds, 'WEEK', 'en_US') as WEEK "+
                "insert into outputStream;");
        String query2 = ("@info(name = 'query2') " +
                "from inputStream " +
                "select symbol , time:extract(timestampInMilliseconds, 'WEEK', 'fr_FR') as WEEK "+
                "insert into outputStream2;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition
                + query + query2);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueEN, inEvent.getData(1));
                }
            }
        });

        executionPlanRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueFR, inEvent.getData(1));
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2017-10-8", "yyyy-MM-dd",1507401000000L});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension16() throws InterruptedException {

        log.info("extractAttributesFunctionExtension16: " +
                "<int>  time: extract (<long> timestampInMilliseconds ,<string>  unit)");
        SiddhiManager siddhiManager = new SiddhiManager();
        Calendar calendarEN = Calendar.getInstance();
        calendarEN.setTimeInMillis(1507401000000L);
        final Integer valueEN =  calendarEN.get(Calendar.WEEK_OF_YEAR);

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string,dateFormat string," +
                "timestampInMilliseconds long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract(timestampInMilliseconds, 'WEEK') as WEEK "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition
                + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueEN, inEvent.getData(1));

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2017-10-8", "yyyy-MM-dd",1507401000000L});
        Thread.sleep(100);
        Assert.assertEquals(1, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void extractAttributesFunctionExtension17() throws InterruptedException, ParseException {

        log.info("extractAttributesFunctionExtension17: " +
                "<int>  time: extract (<string> unit ,<string>  dateValue)");
        SiddhiManager siddhiManager = new SiddhiManager();
        FastDateFormat userSpecificFormat;
        userSpecificFormat = FastDateFormat.getInstance(TimeExtensionConstants.EXTENSION_TIME_DEFAULT_DATE_FORMAT);
        Date userSpecifiedDate = userSpecificFormat.parse("2017-10-8 02:23:44.999");
        Calendar calendarEN = Calendar.getInstance();
        calendarEN.setTime(userSpecifiedDate);
        final Integer valueEN =  calendarEN.get(Calendar.WEEK_OF_YEAR);

        String inStreamDefinition = "" +
                "define stream inputStream (symbol string,dateValue string," +
                "timestampInMilliseconds long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select symbol , time:extract('WEEK', dateValue) as WEEK "+
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition
                + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    log.info("Event : " + count + ",WEEK : " + inEvent.getData(1));
                    Assert.assertEquals(valueEN, inEvent.getData(1));
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "2017-10-8 02:23:44.999", 1507401000000L});
        Thread.sleep(100);
        Assert.assertEquals(1, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}

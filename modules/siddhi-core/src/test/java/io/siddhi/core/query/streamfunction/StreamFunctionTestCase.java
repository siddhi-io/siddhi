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
package io.siddhi.core.query.streamfunction;

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

public class StreamFunctionTestCase {
    private static final Logger log = LogManager.getLogger(StreamFunctionTestCase.class);
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
    public void pol2CartFunctionTest() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String polarStream = "define stream PolarStream (theta double, rho double);";
        String query = "@info(name = 'query1') " +
                "from PolarStream#pol2Cart(theta, rho) " +
                "select x, y " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(polarStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertEquals(12, Math.round((Double) inEvents[0].getData(0)));
                    AssertJUnit.assertEquals(5, Math.round((Double) inEvents[0].getData(1)));

                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("PolarStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{22.6, 13.0});
        Thread.sleep(100);
        AssertJUnit.assertEquals(1, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void pol2CartFunctionTest2() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String polarStream = "define stream PolarStream (theta double, rho double, elevation double);";
        String query = "@info(name = 'query1') " +
                "from PolarStream#pol2Cart(theta, rho, elevation) " +
                "select x, z " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(polarStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertEquals(12, Math.round((Double) inEvents[0].getData(0)));
                    AssertJUnit.assertEquals(7, Math.round((Double) inEvents[0].getData(1)));
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("PolarStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{22.6, 13.0, 7.0});
        Thread.sleep(100);
        AssertJUnit.assertEquals(1, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void pol2CartFunctionTest3() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String polarStream = "define stream PolarStream (theta double, rho double);";
        String query = "@info(name = 'query1') " +
                "from PolarStream#pol2Cart(*) " +
                "select x, y " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(polarStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertEquals(12, Math.round((Double) inEvents[0].getData(0)));
                    AssertJUnit.assertEquals(5, Math.round((Double) inEvents[0].getData(1)));
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("PolarStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{22.6, 13.0});
        Thread.sleep(100);
        AssertJUnit.assertEquals(1, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void nonStandardAttribute() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:get", AttributeStreamFunction.class);
        String siddhiApp = "" +
                "define stream `$InputStream` (`56$2theta` double, rho double); " +
                "@info(name = 'query1') " +
                "from `$InputStream`#custom:get('test(0)') " +
                "select `56$2theta`, rho, `test(0)` as foo " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertEquals(22.6, inEvents[0].getData(0));
                    AssertJUnit.assertEquals(13.0, inEvents[0].getData(1));
                    AssertJUnit.assertEquals("test", inEvents[0].getData(2));
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("$InputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{22.6, 13.0});
        Thread.sleep(100);
        AssertJUnit.assertEquals(1, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void logStreamProcessorTest1() throws InterruptedException {
        log.info("logStreamProcessorTest #1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price double, discount int);";
        String execPlan = "@info(name = 'query1') " +
                "from cseEventStream#log(12) " +
                "select* " +
                "insert into outputStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + execPlan);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void logStreamProcessorTest2() throws InterruptedException {
        log.info("logStreamProcessorTest #2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price double, discount int);";
        String execPlan = "@info(name = 'query1') " +
                "from cseEventStream#log(12, 'message') " +
                "select* " +
                "insert into outputStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + execPlan);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void logStreamProcessorTest3() throws InterruptedException {
        log.info("logStreamProcessorTest #3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price double, discount int);";
        String execPlan = "@info(name = 'query1') " +
                "from cseEventStream#log('message', true, 1, 'hai') " +
                "select* " +
                "insert into outputStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + execPlan);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void logStreamProcessorTest4() throws InterruptedException {
        log.info("logStreamProcessorTest #4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price double, discount int);";
        String execPlan = "@info(name = 'query1') " +
                "from cseEventStream#log(12, 'event', true) " +
                "select* " +
                "insert into outputStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + execPlan);
    }
}

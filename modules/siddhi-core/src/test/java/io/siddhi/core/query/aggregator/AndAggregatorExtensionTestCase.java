/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.query.aggregator;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AndAggregatorExtensionTestCase {

    private static final Logger log = LogManager.getLogger(AndAggregatorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private int inEventCount;

    @BeforeMethod
    public void init() {

        count = 0;
        eventArrived = false;
        inEventCount = 0;
    }

    @Test
    public void testAndAggregatorTrueOnlyScenario() throws InterruptedException {

        log.info("AndAggregator TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream cscStream(messageID string, isFraud bool, price double);";
        String query = ("@info(name = 'query1') " +
                "from cscStream#window.lengthBatch(3) " +
                "select messageID, and(isFraud) as isValidTransaction " +
                "group by messageID " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(true, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cscStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        Thread.sleep(2000);
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = "testAndAggregatorTrueOnlyScenario")
    public void testAndAggregatorFalseOnlyScenario() throws InterruptedException {

        log.info("AndAggregator TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream cscStream(messageID string, isFraud bool, price double);";
        String query = ("@info(name = 'query1') " +
                "from cscStream#window.lengthBatch(4) " +
                "select messageID, and(isFraud) as isValidTransaction " +
                "group by messageID " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(false, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cscStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        Thread.sleep(2000);
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testAndAggregatorFalseOnlyScenario")
    public void testAndAggregatorTrueFalseScenario() throws InterruptedException {

        log.info("AndAggregator TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream cscStream(messageID string, isFraud bool, price double);";
        String query = ("@info(name = 'query1') " +
                "from cscStream#window.lengthBatch(4) " +
                "select messageID, and(isFraud) as isValidTransaction " +
                "group by messageID " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(false, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cscStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        Thread.sleep(2000);
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testAndAggregatorTrueFalseScenario")
    public void testAndAggregatorMoreEventsBatchScenario() throws InterruptedException {

        log.info("AndAggregator TestCase 4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream cscStream(messageID string, isFraud bool, price double);";
        String query = ("@info(name = 'query1') " +
                "from cscStream#window.lengthBatch(2) " +
                "select messageID, and(isFraud) as isValidTransaction " +
                "group by messageID " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(false, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(true, event.getData(1));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cscStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"messageId1", false, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        inputHandler.send(new Object[]{"messageId1", true, 35.75});
        Thread.sleep(2000);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void andAggregatorTest5() throws InterruptedException {

        log.info("andAggregator Test 5");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (name string, isFraud bool);";
        String execPlan = "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(1) " +
                "select and(isFraud, name) as isFraudTransaction " +
                "insert into outputStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + execPlan);
    }

    @Test
    public void andAggregatorTest6() throws InterruptedException {

        log.info("andAggregator Test 6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@app:name('andAggregatorTests') " +
                "" +
                "define stream cseEventStream (name string, isFraud bool);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select and(isFraud) as isFraudTransaction " +
                "insert into outputStream;";

        SiddhiAppRuntime execPlanRunTime = siddhiManager.createSiddhiAppRuntime(execPlan);

        log.info("Running: " + execPlanRunTime.getName());

        execPlanRunTime.addCallback("query1", new QueryCallback() {

            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                inEventCount++;
            }
        });

        execPlanRunTime.start();
        execPlanRunTime.shutdown();
        AssertJUnit.assertEquals(0, inEventCount);
    }

    @Test
    public void andAggregatorTest7() throws InterruptedException {

        log.info("andAggregator Test 7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@app:name('andAggregatorTests') " +
                "" +
                "define stream cseEventStream (name string, isFraud bool);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select and(isFraud) as isFraudTransaction " +
                "insert into outputStream;";

        SiddhiAppRuntime execPlanRunTime = siddhiManager.createSiddhiAppRuntime(execPlan);
        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(true, inEvents[0].getData()[0]);
            }
        });

        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");

        execPlanRunTime.start();
        inputHandler.send(new Object[]{"Nayana", "True"});
        Thread.sleep(100);
        execPlanRunTime.shutdown();
    }

}

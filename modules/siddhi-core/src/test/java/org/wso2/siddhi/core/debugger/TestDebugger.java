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
package org.wso2.siddhi.core.debugger;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.debugger.extension.CustomSumExtension;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by bhagya on 7/21/16.
 */
public class TestDebugger {
    private static final Logger log = Logger.getLogger(TestDebugger.class);
    private AtomicInteger inEventCount = new AtomicInteger(0);
    private int removeEventCount;
    private boolean eventArrived;
    private static volatile int count;

    public void init() {
        inEventCount.set(0);
        removeEventCount = 0;
        eventArrived = false;
    }


    @Test
    public void testNext() {
        log.info("testNext");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.length(5) " +
                "select symbol,price,volume " +
                "insert into largerThanFiftyStream; " +
                "" +
                "@info(name = 'query2') " +
                "from largerThanFiftyStream#window.length(5)[volume > 100l] " +
                "select symbol,price,volume " +
                "insert into largerThanHundredStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("largerThanHundredStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count = count + events.length;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1","in");
        SiddhiTestCallback siddhiTestCallback = new SiddhiTestCallback();
        siddhiDebugger.setDebuggerCallback(siddhiTestCallback);
        List<Object[]> sendingEvents = new ArrayList<Object[]>();
        sendingEvents.add(new Object[]{"WSO2", 50f, 60});
        sendingEvents.add(new Object[]{"WSO2", 70f, 40});
        Runnable senderThread = new AsyncThread(inputHandler, sendingEvents);
        new Thread(senderThread).start();
        waitForEvents(siddhiTestCallback);
        Object[] results = new Object[]{"WSO2", 50f, 60};
        assertArrayEquals(results, siddhiTestCallback.getCurrentEvents());
        siddhiDebugger.next();
        waitForEvents(siddhiTestCallback);
        results = new Object[]{"WSO2", 50f, 60};
        assertArrayEquals(results, siddhiTestCallback.getCurrentEvents());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted Exception", e);
        }

        executionPlanRuntime.shutdown();

    }

    @Test
    public void testPlay() {
        log.info("testPlay");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.length(5) " +
                "select symbol,price,volume insert into largerThanFiftyStream;" +
                "@info(name = 'query2') from largerThanFiftyStream#window.length(5)[volume > 100l] select symbol,price,volume insert into largerThanHundredStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("largerThanHundredStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count = count + events.length;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1","in");
        SiddhiTestCallback siddhiTestCallback = new SiddhiTestCallback();
        siddhiDebugger.setDebuggerCallback(siddhiTestCallback);
        List<Object[]> sendingEvents = new ArrayList<Object[]>();
        sendingEvents.add(new Object[]{"WSO2", 50f, 60});
        sendingEvents.add(new Object[]{"WSO2", 40f, 60});
        Runnable senderThread = new AsyncThread(inputHandler, sendingEvents);
        new Thread(senderThread).start();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.play();
        waitForEvents(siddhiTestCallback);
        Object[] results = {"WSO2", 40f, 60};
        assertArrayEquals(results, siddhiTestCallback.getCurrentEvents());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted Exception", e);
        }
        executionPlanRuntime.shutdown();


    }

    @Test
    public void testGetState() {
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume int);";
        String cseEventStream2 = "@config(async = 'true') define stream cseEventStream2 (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.length(5) as cse1 " +
                "join cseEventStream2#window.length(5) as cse2 " +
                "on cse1.symbol==cse2.symbol " +
                "select cse1.symbol " +
                "insert into largerThanHundredStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + cseEventStream2 + query);

        executionPlanRuntime.addCallback("largerThanHundredStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count = count + events.length;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler("cseEventStream2");
        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        SiddhiTestCallback siddhiTestCallback = new SiddhiTestCallback();
        siddhiDebugger.setDebuggerCallback(siddhiTestCallback);
        siddhiDebugger.acquireBreakPoint("query1","in");
        List<Object[]> sendingEventsForHandler1 = new ArrayList<Object[]>();
        sendingEventsForHandler1.add(new Object[]{"WSO2", 50f, 60});
        sendingEventsForHandler1.add(new Object[]{"WSO2", 70f, 40});
        sendingEventsForHandler1.add(new Object[]{"WSO2", 70f, 45});
        sendingEventsForHandler1.add(new Object[]{"IBM", 44f, 250});
        sendingEventsForHandler1.add(new Object[]{"WSO2", 72f, 400});
        Runnable senderThread1 = new AsyncThread(inputHandler, sendingEventsForHandler1);
        new Thread(senderThread1).start();
        List<Object[]> sendingEventsForHandler2 = new ArrayList<Object[]>();
        sendingEventsForHandler2.add(new Object[]{"IBM", 50f, 60});
        sendingEventsForHandler2.add(new Object[]{"IBM", 70f, 40});
        sendingEventsForHandler2.add(new Object[]{"WSO2", 44f, 200});
        sendingEventsForHandler2.add(new Object[]{"WSO2", 66f, 250});
        Runnable senderThread2 = new AsyncThread(inputHandler2, sendingEventsForHandler2);
        new Thread(senderThread2).start();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.next();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.next();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.next();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.getQueryState("query1");
        QueryState receivedQueryState=siddhiTestCallback.getQueryStates();
        Assert.assertEquals(true, verifyState(receivedQueryState, "AbstractStreamProcessor", "outputData=[WSO2]"));

        executionPlanRuntime.shutdown();

    }

    @Test
    public void testCustomSumExtension() {
        log.info("testCustomExtension");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("custom:sum", CustomSumExtension.class);
        String cseEventStream = "define stream cseEventStream (symbol string, price double, volume double);";
        String query = ("@info(name = 'query1') from cseEventStream select symbol , custom:sum(price,volume) as totalCount " +
                "insert into mailOutput;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        executionPlanRuntime.addCallback("mailOutput", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count = count + events.length;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        SiddhiTestCallback siddhiTestCallback = new SiddhiTestCallback();
        siddhiDebugger.setDebuggerCallback(siddhiTestCallback);
        siddhiDebugger.acquireBreakPoint("query1", "in");
        List<Object[]> sendingEventsForHandler1 = new ArrayList<Object[]>();
        sendingEventsForHandler1.add(new Object[]{"WSO2", 50.0, 60.0});
        sendingEventsForHandler1.add(new Object[]{"WSO2", 70.0, 40.0});
        sendingEventsForHandler1.add(new Object[]{"WSO2", 70.0, 45.0});
        sendingEventsForHandler1.add(new Object[]{"IBM", 44.0, 250.0});
        Runnable senderThread1 = new AsyncThread(inputHandler, sendingEventsForHandler1);
        new Thread(senderThread1).start();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.play();
        waitForEvents(siddhiTestCallback);
        siddhiDebugger.play();
        siddhiDebugger.getQueryState("query1");

        QueryState receivedQueryState = siddhiTestCallback.getQueryStates();
        Assert.assertEquals(true, verifyState(receivedQueryState, "FunctionExecutor", "220.0"));
    }

    private void waitForEvents(SiddhiTestCallback siddhiDebuggerCallback) {
        while (!siddhiDebuggerCallback.isEventReceived()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted Exception", e);
            }
        }
        siddhiDebuggerCallback.setEventReceived(false);
    }

    public boolean verifyState(QueryState queryState, String expectedSnapshotableClass, String expectedOutputData) {
        HashMap<String, Map<String, Object>> knownFields = queryState.getKnownFields();
        HashMap<String, Object[]> unknownFields = queryState.getUnknownFields();
        boolean result = false;
        String actualSnapshotableClass;
        Map<String, Object> snapShots;
        for (Map.Entry<String, Map<String, Object>> entry : knownFields.entrySet()) {
            actualSnapshotableClass = entry.getKey();
            if (actualSnapshotableClass.contains(expectedSnapshotableClass)) {
                snapShots = entry.getValue();
                for (Map.Entry<String, Object> snapShotEntry : snapShots.entrySet()) {
                    if (entry.getValue().toString().contains(expectedOutputData)) {
                        result = true;
                    }
                }
            }
        }
        if (!result) {
            for (Map.Entry<String, Object[]> unknownfield : unknownFields.entrySet()) {
                if (unknownfield.getKey().contains(expectedSnapshotableClass)) {
                    for (Object object : unknownfield.getValue()) {
                        if (object.toString().contains(expectedOutputData)) {
                            result = true;
                        }
                    }

                }
            }
        }


        return result;
    }
}

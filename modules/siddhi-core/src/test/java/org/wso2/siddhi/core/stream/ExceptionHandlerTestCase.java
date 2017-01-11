/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.siddhi.core.stream;

import com.lmax.disruptor.ExceptionHandler;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

public class ExceptionHandlerTestCase {

    static final Logger log = Logger.getLogger(CallbackTestCase.class);
    private volatile AtomicInteger count;
    private volatile boolean eventArrived;
    private volatile AtomicInteger failedCount;
    private volatile boolean failedCaught;
    private SiddhiManager siddhiManager;

    @Before
    public void init() {
        count = new AtomicInteger(0);
        eventArrived = false;
        failedCount = new AtomicInteger(0);
        failedCaught = false;
    }

    private ExecutionPlanRuntime createTestExecutionRuntime(){
        siddhiManager = new SiddhiManager();
        String executionPlan = "" +
                "@Plan:name('callbackTest1') " +
                "@Plan:async " +
                "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "@Parallel " +
                "from StockStream[price + 0.0 > 0.0] " +
                "select symbol, price " +
                "insert into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        return executionPlanRuntime;
    }

    /**
     * Send 6 test events (2 valid -> 2 invalid -> 2 valid)
     *
     * @param inputHandler input handler
     * @throws Exception
     */
    private void sendTestInvalidEvents(InputHandler inputHandler ) throws Exception {
        // Send 2 valid events
        inputHandler.send(new Object[]{"GOOD_0", 700.0f, 100l});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"GOOD_1", 60.5f, 200l});
        Thread.sleep(100);
        try {
            // Send 2 invalid event
            inputHandler.send(new Object[]{"BAD_2", "EBAY", 200l});
            Thread.sleep(100);
            inputHandler.send(new Object[]{"BAD_3", "WSO2",700f});
            Thread.sleep(100);
        }catch (Exception ex){
            Assert.fail("Disruptor exception can't be caught by try-catch");
            throw ex;
        }
        // Send 2 valid events
        inputHandler.send(new Object[]{"GOOD_4", 700.0f, 100l});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"GOOD_5", 60.5f, 200l});
        Thread.sleep(100);
    }

    private void sendTestValidEvents(InputHandler inputHandler ) throws Exception {
        inputHandler.send(new Object[]{"IBM", 700.0f, 100l});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
        inputHandler.send(new Object[]{"IBM", 700.0f, 100l});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
        inputHandler.send(new Object[]{"IBM", 700.0f, 100l});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
    }

    @Test
    public void callbackTestForValidEvents() throws Exception {
        log.info("callback test without exception handler");
        ExecutionPlanRuntime executionPlanRuntime = createTestExecutionRuntime();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        sendTestValidEvents(inputHandler);

        SiddhiTestHelper.waitForEvents(100, 6, count, 60000);

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(6, count.get());
        Assert.assertFalse(failedCaught);
        Assert.assertEquals(0, failedCount.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void callbackTestForInvalidEventWithExceptionHandler() throws Exception {
        log.info("callback test with exception handler");
        ExecutionPlanRuntime executionPlanRuntime = createTestExecutionRuntime();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.handleExceptionWith(new ExceptionHandler<Object>() {
            @Override
            public void handleEventException(Throwable throwable, long l, Object o) {
                failedCount.incrementAndGet();
                failedCaught = true;
                log.info(o+": properly handle event exception for bad event [sequence: " + l + ", failed: "+failedCount+"]", throwable);
            }

            @Override
            public void handleOnStartException(Throwable throwable) {
                failedCount.incrementAndGet();
                failedCaught = true;
            }

            @Override
            public void handleOnShutdownException(Throwable throwable) {
                log.info("Properly handle shutdown exception", throwable);
                failedCount.incrementAndGet();
                failedCaught = true;
            }
        });

        executionPlanRuntime.start();
        sendTestInvalidEvents(inputHandler);
        SiddhiTestHelper.waitForEvents(100, 2, failedCount, 60000);

        // No following events can be processed correctly
        Assert.assertTrue("Should properly process all the 4 valid events",eventArrived);
        Assert.assertEquals("Should properly process all the 4 valid events",4, count.get());
        Assert.assertTrue("Exception is properly handled thrown by 2 invalid events",failedCaught);
        Assert.assertEquals("Exception is properly handled thrown by 2 invalid events",2, failedCount.get());
        executionPlanRuntime.shutdown();
    }
}
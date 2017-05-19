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

package org.wso2.siddhi.core.query.pattern;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

public class AbsentPatternTestCase {

    private static final Logger log = Logger.getLogger(AbsentPatternTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser1() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser2() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 within 2 sec' parser of pattern for absence of events with select e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 2 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser3() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 within 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }


    @Test
    public void testQueryParser4() throws InterruptedException {
        log.info("Test the query 'every e1 -> not e2 within 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price>e1.price] within 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser5() throws InterruptedException {
        log.info("Test the query 'every e1 -> not e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price>e1.price] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser6() throws InterruptedException {
        log.info("Test the query 'not e1 -> not e2 within 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] -> not Stream2[price>30] within 2 sec " +
                "select 25.0f as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser7() throws InterruptedException {
        log.info("Test the query 'not e1 -> not e2 within 2 sec -> e3' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 2 sec -> " +
                "e3=Stream3[symbol==e1.symbol] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser8() throws InterruptedException {
        log.info("Test the query 'e1 -> e3 -> not e2 within 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> e3=Stream3[symbol==e1.symbol] -> not Stream2[price>e1.price] within 2 " +
                "sec " +
                "select e1.symbol as symbol, e3.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser9() throws InterruptedException {
        log.info("Test the query 'not e1 within 2 sec -> e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] within 2 sec -> e2=Stream2[symbol=='IBM'] " +
                "select e2.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser10() throws InterruptedException {
        log.info("Test the query 'every not e1 within 2 sec -> e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every not Stream1[price>20] within 2 sec -> e2=Stream2[symbol=='IBM'] " +
                "select e2.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser11() throws InterruptedException {
        log.info("Test the query 'not e1 within 2 sec -> not e2 within 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not e1=Stream1[price>20] within 2 sec -> not Stream2[price>e1.price] within 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser12() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 within 2 sec and e3' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price > 20] -> not Stream2[price > e1.price] and e3=Stream2['IBM' == " +
                "symbol] " +
                "select e1.symbol as symbol1, e3.price as price3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent1() throws InterruptedException {
        log.info("Test the query e1 -> not e2 without sending e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 1 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent2() throws InterruptedException {
        log.info("Test the query e1 -> not e2 sending e2 after 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 1 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(1100);
        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent3() throws InterruptedException {
        log.info("Test the query e1 -> not e2 sending e2 within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 1 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent4() throws InterruptedException {
        log.info("Test the query e1 -> not e2 sending e2 after 1 sec but without satisfying the filter condition");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] within 1 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 50.7f, 100});
        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent5() throws InterruptedException {
        log.info("Test the query not e1 -> e2 without e1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] within 1 sec -> e2=Stream2[price>30] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent6() throws InterruptedException {
        log.info("Test the query not e1 -> e2 with e1 and e2 after 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] within 1 sec -> e2=Stream2[price>30] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 59.6f, 100});
        Thread.sleep(1100);
        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent7() throws InterruptedException {
        log.info("Test the query not e1 -> e2 with e1 and e2 within 1 sec where e1 filter fails");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] within 1 sec -> e2=Stream2[price>30] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 5.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event not arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent8() throws InterruptedException {
        log.info("Test the query not e1 -> e2 with e1 and e2 within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] within 1 sec -> e2=Stream2[price>30] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 58.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent9() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> not e3 with e1, e2 and e3 within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 sec " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent10() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> not e3 with e1, e2 and e3 which does not meet the condition within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 sec " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 25.7f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent11() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> not e3 with e1, e2 and not e3 within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 sec " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }


    @Test
    public void testQueryAbsent12() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec -> e3 with e1 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec -> e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        Thread.sleep(1100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent13() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec -> e3 with e1, e2(condition failed) e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec -> e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 8.7f, 100});
        Thread.sleep(1100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent14() throws InterruptedException {
        log.info("Test the query e1 -> not e2 within 1 sec -> e3 with e1, e2 e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> not Stream2[price>20] within 1 sec -> e3=Stream3[price>30] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent15() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> e3 with e1, e2 e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> e3=Stream3[price>30] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent16() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> e3 with e2 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> e3=Stream3[price>30] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent17() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> e3 with e1 that fails to satisfy the condition, e2 and " +
                "e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> e3=Stream3[price>30] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 5.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent18() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> e3 with e1, e2 after 1 sec and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> e3=Stream3[price>30] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(1100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent19() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> e3 -> not e4 within 1 sec with e1, e2, e3 and not e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> e3=Stream3[price>30] -> not Stream4[price>40] " +
                "within 1 sec  " +
                "select e1.symbol as symbol1, e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.7f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent20() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> e3 -> not e4 within 1 sec with e1, e2, e3 and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> e3=Stream3[price>30] -> not Stream4[price>40] " +
                "within 1 sec  " +
                "select e1.symbol as symbol1, e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 35.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(1100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent21() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> not e3 within 1 sec -> e4 with e1, e2, and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 sec -> " +
                "e4=Stream4[price>40] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(1100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent22() throws InterruptedException {
        log.info("Test the query e1 -> e2 -> not e3 within 1 sec -> e4 with e1, e2, e3, and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>10] -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 sec -> " +
                "e4=Stream4[price>40] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(1100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent23() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> e3-> e4 with e1, e2, e3, and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> e3=Stream3[price>30] -> " +
                "e4=Stream4[price>40] " +
                "select e2.symbol as symbol2, e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent24() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e3, and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent25() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e1, e2, e3, and e4 all " +
                "within 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent26() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e2, e3, and e4 all within" +
                " 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent27() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e1, e3, and e4 all within" +
                " 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent28() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e1 and after 1 sec e3, " +
                "and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(1100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent29() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> not e2 within 1 sec -> e3-> e4 with e1, e2 and after 1 sec " +
                "e3, and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> not Stream2[price>20] within 1 sec -> " +
                "e3=Stream3[price>30] -> e4=Stream4[price>40] " +
                "select e3.symbol as symbol3, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(1100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent30() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> not e3 within 1 sec-> e4 with e2 after 1 sec e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 " +
                "sec -> e4=Stream4[price>40] " +
                "select e2.symbol as symbol2, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();


        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(1100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertTrue("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent31() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> not e3 within 1 sec-> e4 with e1, e2, e3 and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 " +
                "sec -> e4=Stream4[price>40] " +
                "select e2.symbol as symbol2, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 15.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryAbsent32() throws InterruptedException {
        log.info("Test the query not e1 within 1 sec -> e2 -> not e3 within 1 sec-> e4 with e2, e3 and e4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream4 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] within 1 sec -> e2=Stream2[price>20] -> not Stream3[price>30] within 1 " +
                "sec -> e4=Stream4[price>40] " +
                "select e2.symbol as symbol2, e4.symbol as symbol4 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        addCallback(executionPlanRuntime, "query1");

        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
        InputHandler stream4 = executionPlanRuntime.getInputHandler("Stream4");

        executionPlanRuntime.start();

        stream2.send(new Object[]{"IBM", 28.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOGLE", 38.7f, 100});
        Thread.sleep(100);
        stream4.send(new Object[]{"ORACLE", 44.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 0, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertFalse("Event arrived", eventArrived);

        executionPlanRuntime.shutdown();
    }

    private void addCallback(ExecutionPlanRuntime executionPlanRuntime, String queryName) {
        executionPlanRuntime.addCallback(queryName, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
    }
}

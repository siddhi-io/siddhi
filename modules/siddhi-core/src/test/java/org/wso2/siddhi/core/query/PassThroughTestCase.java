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

package org.wso2.siddhi.core.query;

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
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;

public class PassThroughTestCase {
    static final Logger log = Logger.getLogger(PassThroughTestCase.class);
    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }


    @Test
    public void PassThroughTest1() throws InterruptedException {
        log.info("pass through test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.insertInto("StockQuote");


        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(cseEventStream);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                Assert.assertTrue("IBM".equals(inEvents[0].getData(0)) || "WSO2".equals(inEvents[0].getData(0)));
                count += inEvents.length;
                eventArrived = true;
            }

        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void PassThroughTest2() throws InterruptedException {
        log.info("pass through test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT);
        StreamDefinition cseEventStream1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.insertInto("StockQuote");


        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(cseEventStream);
        executionPlan.defineStream(cseEventStream1);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                count += inEvents.length;
                eventArrived = true;
            }

        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream1");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});
        Thread.sleep(100);
        Assert.assertEquals(0, count);
        Assert.assertFalse(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void PassThroughTest3() throws InterruptedException {
        log.info("pass through test3");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT);
        StreamDefinition cseEventStream1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price",Expression.variable("symbol"))
        );
        query.insertInto("StockQuote");


        ExecutionPlan executionPlan = new ExecutionPlan("ep1");
        executionPlan.defineStream(cseEventStream);
        executionPlan.defineStream(cseEventStream1);
        executionPlan.addQuery(query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                count += inEvents.length;
                eventArrived = true;
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler1 = executionPlanRuntime.getInputHandler("cseEventStream1");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});

        inputHandler1.send(new Object[]{"ORACLE", 100});
        inputHandler1.send(new Object[]{"ABC", 100});

        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void PassThroughTest4() throws InterruptedException {
        log.info("pass through test4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@Plan:name('passThroughTest4') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "insert into outputStream;" +
                "" +
                "@info(name = 'query2') " +
                "from outputStream " +
                "select * " +
                "insert into outputStream2 ;";


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        log.info("Running : " + executionPlanRuntime.getName());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertTrue("WSO2".equals(inEvents[0].getData(0)));
                count = count + inEvents.length;
                eventArrived = true;
            }

        };
        executionPlanRuntime.addCallback("query2", queryCallback);
        queryCallback.startProcessing();

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"WSO2", 700f, 100l});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
        Thread.sleep(100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        queryCallback.stopProcessing();

        executionPlanRuntime.shutdown();
    }

}

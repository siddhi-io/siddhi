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
package org.wso2.siddhi.core.query.partition;

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
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.concurrent.atomic.AtomicInteger;


public class PartitionTestCase {
    static final Logger log = Logger.getLogger(PartitionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private int stockStreamEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
        stockStreamEventCount = 0;
    }

    @Test
    public void testPartitionQuery() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "partition with (symbol of streamA) " +
                "begin " +
                "@info(name = 'query1') " +
                "from streamA select symbol,price insert into StockQuote ;  " +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700});
        inputHandler.send(new Object[]{"WSO2", 60});
        inputHandler.send(new Object[]{"WSO2", 60});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery1() throws InterruptedException {
        log.info("Partition test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest1') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStreamOne (symbol string, price float,volume int);"
                + "@info(name = 'query')from cseEventStreamOne select symbol,price,volume insert into cseEventStream;"
                + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from cseEventStream[700>price] select symbol,sum(price) as price,volume insert into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(75.5999984741211, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(151.1999969482422, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(75.5999984741211, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 70005.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery2() throws InterruptedException {
        log.info("Partition test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest2') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream StockStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream , symbol of StockStream1) begin @info(name = 'query1') from cseEventStream[700>price] select symbol,sum(price) as price,volume insert into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery3() throws InterruptedException {
        log.info("Partition test3");


        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("plan3");

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        executionPlan.defineStream(streamDefinition);

        Partition partition = Partition.partition().
                with("cseEventStream", Expression.variable("symbol"));

        Query query = Query.query();
        query.from(InputStream.stream("cseEventStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query.insertIntoInner("StockStream");


        Query query1 = Query.query();
        query1.from(InputStream.innerStream("StockStream"));
        query1.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query1.insertInto("OutStockStream");

        partition.addQuery(query);
        partition.addQuery(query1);

        executionPlan.addPartition(partition);


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        executionRuntime.shutdown();
        Assert.assertEquals(4, count.get());

    }


    @Test
    public void testPartitionQuery4() throws InterruptedException {
        log.info("Partition test4");
        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("plan4");

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);
        StreamDefinition streamDefinition1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        executionPlan.defineStream(streamDefinition);
        executionPlan.defineStream(streamDefinition1);


        Partition partition = Partition.partition().
                with("cseEventStream", Expression.variable("symbol"));

        Query query = Query.query();
        query.from(InputStream.stream("cseEventStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query.insertIntoInner("StockStream");


        Query query1 = Query.query();
        query1.from(InputStream.innerStream("StockStream"));
        query1.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query1.insertInto("OutStockStream");


        ////////////////////////////////////partition-2/////////////////


        Partition partition1 = Partition.partition().
                with("cseEventStream1", Expression.variable("symbol"));

        Query query2 = Query.query();
        query2.from(InputStream.stream("cseEventStream1"));
        query2.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query2.insertIntoInner("StockStream");


        Query query3 = Query.query();
        query3.from(InputStream.innerStream("StockStream"));
        query3.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query3.insertInto("OutStockStream");


        partition.addQuery(query);
        partition.addQuery(query1);

        partition1.addQuery(query2);
        partition1.addQuery(query3);

        executionPlan.addPartition(partition);

        executionPlan.addPartition(partition1);

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });


        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler2 = executionRuntime.getInputHandler("cseEventStream1");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});

        inputHandler2.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler2.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler2.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler2.send(new Object[]{"ORACLE", 75.6f, 100});

        SiddhiTestHelper.waitForEvents(100, 8, count, 60000);
        executionRuntime.shutdown();
        Assert.assertEquals(8, count.get());

    }

    @Test
    public void testPartitionQuery5() throws InterruptedException {
        log.info("Partition test5");
        final SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("plan5");

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);
        StreamDefinition streamDefinition1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);
        StreamDefinition streamDefinition2 = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);


        executionPlan.defineStream(streamDefinition);
        executionPlan.defineStream(streamDefinition1);
        executionPlan.defineStream(streamDefinition2);


        Partition partition = Partition.partition().
                with("cseEventStream", Expression.variable("symbol"));

        Query query = Query.query();
        query.from(InputStream.stream("cseEventStream")).annotation(Annotation.annotation("info").element("name", "query"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query.insertIntoInner("StockStream");

        Query query1 = Query.query();
        query1.from(InputStream.innerStream("StockStream")).annotation(Annotation.annotation("info").element("name", "query1"));
        query1.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query1.insertInto("OutStockStream");


        ////////////////////////////////////partition-2/////////////////


        Partition partition1 = Partition.partition().
                with("cseEventStream1", Expression.variable("symbol"));

        Query query2 = Query.query();
        query2.from(InputStream.stream("cseEventStream1")).annotation(Annotation.annotation("info").element("name", "query2"));
        query2.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query2.insertIntoInner("StockStream");


        Query query3 = Query.query();
        query3.from(InputStream.innerStream("StockStream")).annotation(Annotation.annotation("info").element("name", "query3"));
        query3.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query3.insertInto("OutStockStream");


        Query query4 = Query.query();
        query4.from(InputStream.stream("StockStream")).annotation(Annotation.annotation("info").element("name", "query4"));
        query4.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query4.insertInto("OutStockStream");


        Query query5 = Query.query();
        query5.from(InputStream.innerStream("StockStream")).annotation(Annotation.annotation("info").element("name", "query5"));
        query5.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))

        );
        query5.insertInto("StockStream");

        partition.addQuery(query);
        partition.addQuery(query1);
        partition.addQuery(query5);

        partition1.addQuery(query2);
        partition1.addQuery(query3);

        executionPlan.addPartition(partition);

        executionPlan.addPartition(partition1);

        executionPlan.addQuery(query4);

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("StockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                stockStreamEventCount = stockStreamEventCount + events.length;

            }
        });


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                synchronized (siddhiManager) {
                    count.addAndGet(events.length);
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler2 = executionRuntime.getInputHandler("cseEventStream1");
        InputHandler inputHandler3 = executionRuntime.getInputHandler("StockStream");

        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});

        inputHandler2.send(new Object[]{"IBM1", 75.6f, 100});
        inputHandler2.send(new Object[]{"WSO21", 75.6f, 100});
        inputHandler2.send(new Object[]{"IBM1", 75.6f, 100});
        inputHandler2.send(new Object[]{"ORACLE1", 75.6f, 100});

        inputHandler3.send(new Object[]{"ABC", 75.6f, 100});
        inputHandler3.send(new Object[]{"DEF", 75.6f, 100});
        inputHandler3.send(new Object[]{"KLM", 75.6f, 100});
        inputHandler3.send(new Object[]{"ABC", 75.6f, 100});

        SiddhiTestHelper.waitForEvents(100, 16, count, 60000);
        Assert.assertEquals(16, count.get());
        Thread.sleep(100);
        Assert.assertEquals(8, stockStreamEventCount);
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery6() throws InterruptedException {
        log.info("Partition test6");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest6') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream , symbol of cseEventStream1) begin @info(name = 'query') from cseEventStream select symbol,price,volume insert into #StockStream ;"
                + "@info(name = 'query1') from #StockStream select symbol,price,volume insert into OutStockStream ;"
                + "@info(name = 'query2') from cseEventStream1 select symbol,price,volume insert into  #StockStream1 ;"
                + "@info(name = 'query3') from #StockStream1 select symbol,price,volume insert into OutStockStream ; end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });


        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler2 = executionRuntime.getInputHandler("cseEventStream1");

        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});

        inputHandler2.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler2.send(new Object[]{"WSO21", 75.6f, 100});
        inputHandler2.send(new Object[]{"IBM1", 75.6f, 100});
        inputHandler2.send(new Object[]{"ORACLE1", 75.6f, 100});

        SiddhiTestHelper.waitForEvents(100, 8, count, 60000);
        Assert.assertEquals(8, count.get());

        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery7() throws InterruptedException {
        log.info("Partition test7");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest7') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream select symbol,sum(price) as price,volume insert into OutStockStream ;"
                + "end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(75.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(705.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(110.0, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(50.0, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"WSO2", 705f, 100});
        inputHandler.send(new Object[]{"IBM", 35f, 100});
        inputHandler.send(new Object[]{"ORACLE", 50.0f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery8() throws InterruptedException {
        log.info("Partition test8");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest8') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream select symbol,max(price) as max_price,volume insert into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(75.0f, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(705.0f, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(75.0f, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(50.0f, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"WSO2", 705f, 100});
        inputHandler.send(new Object[]{"IBM", 35f, 100});
        inputHandler.send(new Object[]{"ORACLE", 50.0f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery9() throws InterruptedException {
        log.info("Partition test9");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest9') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream select symbol,min(price) as min_price,volume insert into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(75.0f, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(705.0f, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(35.0f, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(50.0f, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"WSO2", 705f, 100});
        inputHandler.send(new Object[]{"IBM", 35f, 100});
        inputHandler.send(new Object[]{"ORACLE", 50.0f, 100});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery10() throws InterruptedException {
        log.info("Partition test10");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest10') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream select symbol,avg(price) as avgPrice,volume insert into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(75.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(705.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(55.0, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(50.0, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"WSO2", 705f, 100});
        inputHandler.send(new Object[]{"IBM", 35f, 100});
        inputHandler.send(new Object[]{"ORACLE", 50.0f, 100});
        SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery11() throws InterruptedException {
        log.info("Partition test11");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest11') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream select count(symbol) as entries insert into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(1l, event.getData()[0]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(1l, event.getData()[0]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(2l, event.getData()[0]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(1l, event.getData()[0]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"WSO2", 705f, 100});
        inputHandler.send(new Object[]{"IBM", 35f, 100});
        inputHandler.send(new Object[]{"ORACLE", 50.0f, 100});
        SiddhiTestHelper.waitForEvents(200, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }


    @Test
    public void testPartitionQuery15() throws InterruptedException {
        log.info("Partition test15");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest15') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "define stream StockStream (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream) begin @info(name = 'query') from cseEventStream select symbol,price as price,volume insert into #StockStream ;"
                + "@info(name = 'query1') from #StockStream select symbol,price,volume insert into OutStockStream ;"
                + "@info(name = 'query2') from #StockStream select symbol,price,volume insert into StockStream ; end ;"
                + "partition with (symbol of cseEventStream1) begin @info(name = 'query3') from cseEventStream1 select symbol,price+5 as price,volume insert into #StockStream ;"
                + "@info(name = 'query4') from #StockStream select symbol,price,volume insert into OutStockStream ; end ;"
                + "@info(name = 'query5') from StockStream select symbol,price+15  as price,volume group by symbol insert into OutStockStream ;";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("StockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                stockStreamEventCount = stockStreamEventCount + events.length;

            }
        });

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler2 = executionRuntime.getInputHandler("cseEventStream1");
        InputHandler inputHandler3 = executionRuntime.getInputHandler("StockStream");

        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});

        inputHandler2.send(new Object[]{"IBM1", 75.6f, 100});
        inputHandler2.send(new Object[]{"WSO21", 75.6f, 100});
        inputHandler2.send(new Object[]{"IBM1", 75.6f, 100});
        inputHandler2.send(new Object[]{"ORACLE1", 75.6f, 100});

        inputHandler3.send(new Object[]{"ABC", 75.6d, 100});
        inputHandler3.send(new Object[]{"DEF", 75.6d, 100});
        inputHandler3.send(new Object[]{"KLM", 75.6d, 100});
        inputHandler3.send(new Object[]{"ABC", 75.6d, 100});

        SiddhiTestHelper.waitForEvents(100, 16, count, 60000);
        executionRuntime.shutdown();
        Assert.assertEquals(16, count.get());
        Assert.assertEquals(8, stockStreamEventCount);
    }

    @Test
    public void testPartitionQuery16() throws InterruptedException {
        log.info("partition test16");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest16') " +
                "define stream streamA (symbol string, price int);"
                + "partition with (symbol of streamA) begin @info(name = 'query1') from streamA select symbol,price insert into StockQuote ;"
                + "@info(name = 'query2') from streamA select symbol,price insert into StockQuote ; end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionPlanRuntime.addCallback("StockQuote", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700});
        inputHandler.send(new Object[]{"WSO2", 60});
        inputHandler.send(new Object[]{"WSO2", 60});
        SiddhiTestHelper.waitForEvents(100, 6, count, 60000);
        Assert.assertEquals(6, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test
    public void testPartitionQuery17() throws InterruptedException {
        log.info("Partition test17");


        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlan executionPlan = new ExecutionPlan("plan17");

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        executionPlan.defineStream(streamDefinition);

        Partition partition = Partition.partition().
                with("cseEventStream",
                        Partition.range("LessValue",
                                Expression.compare(
                                        Expression.value(200),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("volume"))
                        ),
                        Partition.range("HighValue",
                                Expression.compare(
                                        Expression.value(200),
                                        Compare.Operator.LESS_THAN_EQUAL,
                                        Expression.variable("volume"))
                        ));

        Query query = Query.query();
        query.from(InputStream.stream("cseEventStream"));

        query.select(
                Selector.selector().
                        select("sumvolume", Expression.function("sum", Expression.variable("volume")))

        );
        query.insertInto("StockStream");


        partition.addQuery(query);


        executionPlan.addPartition(partition);


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("StockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(100l, event.getData()[0]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(600l, event.getData()[0]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(200l, event.getData()[0]);
                    } else if (count.get() == 4) {
                        Assert.assertEquals(250l, event.getData()[0]);
                    }
                    eventArrived = true;
                }
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 600});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 50});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();

    }


    @Test
    public void testPartitionQuery18() throws InterruptedException {
        log.info("Partition test18");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest18') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStreamOne (symbol string, price float,volume int);"
                + "@info(name = 'query')from cseEventStreamOne select symbol,price,volume insert into cseEventStream;"
                + "partition with (price>=100 as 'large' or price<100 as 'small' of cseEventStream) begin @info(name = 'query1') from cseEventStream#window.length(4) select symbol,sum(price) as price insert into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(7005.60009765625, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertTrue(event.getData()[1].equals(75.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 4) {
                        Assert.assertEquals(100.0, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 25f, 100});
        inputHandler.send(new Object[]{"WSO2", 7005.6f, 100});
        inputHandler.send(new Object[]{"IBM", 50f, 100});
        inputHandler.send(new Object[]{"ORACLE", 25f, 100});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertTrue(count.get() <= 4);
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery19() throws InterruptedException {
        log.info("Partition test19");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest19') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStreamOne (symbol string, price float,volume int);"
                + "@info(name = 'query')from cseEventStreamOne select symbol,price,volume insert into cseEventStream;"
                + "partition with (price>=100 as 'large' or price<100 as 'medium' or price<50 as 'small' of cseEventStream) begin @info(name = 'query1') from cseEventStream select symbol,sum(price) as price insert into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(7005.60009765625, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertTrue(event.getData()[1].equals(75.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 5) {
                        Assert.assertTrue(event.getData()[1].equals(50.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 6) {
                        Assert.assertEquals(50.0, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();

        inputHandler.send(new Object[]{"IBM", 25f, 100});
        inputHandler.send(new Object[]{"WSO2", 7005.6f, 100});
        inputHandler.send(new Object[]{"IBM", 50f, 100});
        inputHandler.send(new Object[]{"ORACLE", 25f, 100});
        SiddhiTestHelper.waitForEvents(100, 5, count, 60000);
        Assert.assertTrue(6 >= count.get());
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery20() throws InterruptedException {
        log.info("Partition test20");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:name('PartitionTest20') " +
                "" +
                "" +
                "define stream cseEventStream (symbol string, price float,volume int); " +
                "" +
                "define stream cseEventStreamOne (symbol string, price float,volume int);" +
                "" +
                "@info(name = 'query')" +
                "from cseEventStreamOne " +
                "select symbol, price, volume " +
                "insert into cseEventStream;" +
                " " +
                "partition with (price>=100 as 'large' or price<100 as 'medium' or price<50 as 'small' of cseEventStream) " +
                "   begin" +
                "   @info(name = 'query1') " +
                "   from cseEventStream " +
                "   select symbol, sum(price) as price " +
                "   insert into #OutStockStream1 ; " +
                " " +
                "   @info(name = 'query2') " +
                "   from #OutStockStream1 " +
                "   insert into #OutStockStream2 ;" +
                " " +
                "   @info(name = 'query3') " +
                "   from #OutStockStream2 " +
                "   insert into OutStockStream ;" +
                " " +
                "   end ; ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(7005.60009765625, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertTrue(event.getData()[1].equals(75.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 5) {
                        Assert.assertTrue(event.getData()[1].equals(50.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 6) {
                        Assert.assertEquals(50.0, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();

        inputHandler.send(new Object[]{"IBM", 25f, 100});
        inputHandler.send(new Object[]{"WSO2", 7005.6f, 100});
        inputHandler.send(new Object[]{"IBM", 50f, 100});
        inputHandler.send(new Object[]{"ORACLE", 25f, 100});
        SiddhiTestHelper.waitForEvents(100, 5, count, 60000);
        Assert.assertTrue(6 >= count.get());
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery21() throws InterruptedException {
        log.info("Partition test21");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:name('PartitionTest20') " +
                "" +
                "" +
                "define stream cseEventStream (symbol string, price float,volume int); " +
                "" +
                "define stream cseEventStreamOne (symbol string, price float,volume int);" +
                "" +
                "@info(name = 'query')" +
                "from cseEventStreamOne " +
                "select symbol, price, volume " +
                "insert into cseEventStream;" +
                " " +
                "partition with (price>=100 as 'large' or price<100 as 'medium' or price<50 as 'small' of cseEventStream) " +
                "   begin" +
                "   @info(name = 'query1') " +
                "   from cseEventStream " +
                "   select symbol, sum(price) as price " +
                "   insert into #OutStockStream1 ; " +
                " " +
                "   @info(name = 'query2') " +
                "   from #OutStockStream1 " +
                "   insert into #OutStockStream2 ;" +
                " " +
                "   @info(name = 'query3') " +
                "   from #OutStockStream2 " +
                "   insert into OutStockStream ;" +
                " " +
                "   end ; ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    eventArrived = true;
                    if (count.get() == 1) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 2) {
                        Assert.assertEquals(25.0, event.getData()[1]);
                    } else if (count.get() == 3) {
                        Assert.assertEquals(7005.60009765625, event.getData()[1]);
                    } else if (count.get() == 4) {
                        Assert.assertTrue(event.getData()[1].equals(50.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 5) {
                        Assert.assertTrue(event.getData()[1].equals(50.0) || event.getData()[1].equals(100.0));
                    } else if (count.get() == 6) {
                        Assert.assertEquals(50.0, event.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStreamOne");
        executionRuntime.start();

        inputHandler.send(new Object[]{"IBM", 25f, 100});
        inputHandler.send(new Object[]{"WSO2", 7005.6f, 100});
        inputHandler.send(new Object[]{"ORACLE", 25f, 100});
        SiddhiTestHelper.waitForEvents(100, 5, count, 60000);
        Assert.assertTrue(5 == count.get());
        executionRuntime.shutdown();

    }

    @Test
    public void testPartitionQuery22() throws InterruptedException {
        log.info("Partition test22");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest10') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream#window.time(1 sec) " +
                "select symbol, avg(price) as avgPrice, volume " +
                "having avgPrice > 10" +
                "insert expired events into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(75.0, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        SiddhiTestHelper.waitForEvents(200, 1, count, 60000);
        Assert.assertTrue(1 <= count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery23() throws InterruptedException {
        log.info("Partition test23");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest10') " +
                "define stream cseEventStream (symbol string, price float,volume int);"
                + "define stream cseEventStream1 (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream)"
                + "begin"
                + "@info(name = 'query') from cseEventStream#window.time(1 sec) " +
                "select symbol, avg(price) as avgPrice, volume " +
                "having avgPrice >= 0" +
                "insert expired events into OutStockStream ;"
                + "end ";

        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(0.0, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75f, 100});
        SiddhiTestHelper.waitForEvents(200, 1, count, 60000);
        Assert.assertEquals(1, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery32() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "partition with (symbol of streamA) " +
                "begin " +
                "@info(name = 'query1') " +
                "from streamA  " +
                "select symbol,price insert into StockQuote ;  " +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        Event[] events = {
                new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}),
                new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}),
                new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60})
        };
        inputHandler.send(events);
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery25() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "" +
                "from streamA#window.lengthBatch(3) " +
                "insert into streamB;" +
                "" +
                "partition with (symbol of streamB) " +
                "begin " +
                "@info(name = 'query1') " +
                "from streamB  " +
                "select symbol,price insert into StockQuote ;  " +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery29() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest') " +
                "define stream streamA (symbol string, price int);" +
                "partition with (symbol of streamA) " +
                "begin " +
                "@info(name = 'query1') " +
                "from streamA  " +
                "select symbol,price insert into StockQuote ;  " +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count.get());
        executionPlanRuntime.shutdown();
    }

    // TODO: 26/9/17 Verify the behaviour when partition executors are empty
    /*@Test
    public void testPartitionQuery27() throws InterruptedException {
        log.info("Partition test");
        ExecutionPlan executionPlan = ExecutionPlan.executionPlan("Test")
                .defineStream(
                        StreamDefinition.id("streamA")
                                .attribute("symbol", Attribute.Type.STRING)
                                .attribute("price", Attribute.Type.INT)
                )
                .defineStream(
                        StreamDefinition.id("StockQuote")
                                .attribute("symbol", Attribute.Type.STRING)
                                .attribute("price", Attribute.Type.INT)
                );;
        Query query = Query.query();

        query.from(
                InputStream.stream("streamA")
        ).select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))).insertInto("StockQuote");
        Partition partition = Partition.partition();
        partition.addQuery(query);
        executionPlan.addPartition(partition);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        Event[] events = {
                new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}),
                new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}),
                new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60})
        };
        inputHandler.send(events);
        Thread.sleep(1000);
        Assert.assertEquals(0, count.get());
        executionPlanRuntime.shutdown();
    }*/

    /*@Test
    public void testPartitionQuery28() throws InterruptedException {
        log.info("Partition test");
        ExecutionPlan executionPlan = ExecutionPlan.executionPlan("Test")
                .defineStream(
                        StreamDefinition.id("streamA")
                                .attribute("symbol", Attribute.Type.STRING)
                                .attribute("price", Attribute.Type.INT)
                );
        Query query = Query.query();
        query.from(
                InputStream.stream("streamA")
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")));
        query.insertInto("StockQuote");
        Partition partition = Partition.partition();
        partition.addQuery(query);
        executionPlan.addPartition(partition);

        SiddhiManager siddhiManager = new SiddhiManager();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("streamA");
        executionPlanRuntime.start();
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandler.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        Thread.sleep(1000);
        Assert.assertEquals(0, count.get());
        executionPlanRuntime.shutdown();
    }*/

    @Test
    public void testPartitionQuery31() throws InterruptedException {
        log.info("Partition test");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:async " +
                "@plan:name('PartitionTest') " +
                "define stream streamA (symbol string, price int); " +
                "define stream streamB (symbol string, price int); " +
                "partition with (symbol of streamA, symbol of streamB) " +
                "begin " +
                "@info(name = 'query1') " +
                "from streamA  " +
                "select symbol,price insert into StockQuote ;  " +
                "@info(name = 'query2') " +
                "from streamB  " +
                "select symbol,price insert into StockQuote ;  " +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        StreamCallback streamCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue("IBM".equals(events[0].getData(0)) || "WSO2".equals(events[0].getData(0)));
                count.addAndGet(events.length);
                eventArrived = true;
            }
        };
        executionPlanRuntime.addCallback("StockQuote", streamCallback);

        InputHandler inputHandlerA = executionPlanRuntime.getInputHandler("streamA");
        InputHandler inputHandlerB = executionPlanRuntime.getInputHandler("streamB");

        executionPlanRuntime.start();
        inputHandlerA.send(new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}));
        inputHandlerA.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandlerA.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandlerB.send(new Event(System.currentTimeMillis(),new Object[]{"IBM", 700}));
        inputHandlerB.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        inputHandlerB.send(new Event(System.currentTimeMillis(),new Object[]{"WSO2", 60}));
        SiddhiTestHelper.waitForEvents(100, 6, count, 60000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(6, count.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery24() throws InterruptedException {
        log.info("Partition test24");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest24') " +
                               "define stream cseEventStream (symbol string, price float,volume int,threshold double);"
                               + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from " +
                               "cseEventStream[700>price AND threshold != volume] select symbol,sum(price) as price," +
                               "volume ,threshold" + " insert into " + "OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,100.0});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100,52.0});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,50.0});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100,200.0});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery33() throws InterruptedException {
        log.info("Partition test25");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest25') " +
                               "define stream cseEventStream (symbol string, price float,volume int,threshold double," +
                               "maxPrice long);"
                               + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from " +
                               "cseEventStream[700>price AND threshold != volume AND price != threshold AND maxPrice " +
                               "!= threshold" +
                               "] select " +
                               "symbol,sum(price) as price," +
                               "volume ,threshold" + " insert into " + "OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,100.0, 500l});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100,52.0, 120l});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,50.0,800l});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100,200.0,400l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        executionRuntime.shutdown();
    }

    @Test
    public void testPartitionQuery26() throws InterruptedException {
        log.info("Partition test26");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@plan:name('PartitionTest26') " +
                               "define stream cseEventStream (atr1 string, atr2 float,atr3 int,atr4 double," +
                               "atr5 long, atr6 long, atr7 double, atr8 float ,atr9 bool,atr10 bool, atr11 int);"
                               + "partition with (atr1 of cseEventStream) begin @info(name = 'query1') from " +
                               "cseEventStream[700>atr5 AND atr5 != atr6 AND atr2 != atr3 AND atr6 " +
                               "!= atr3 AND atr2 != atr5 AND atr4 != atr6 AND atr3 != atr4 AND atr4 != atr7 AND atr8 " +
                               "!= atr2 AND atr4 != atr8 AND atr6 != atr8 AND atr3 != atr2 AND atr3 != atr5 AND atr9 " +
                               "!= atr10 AND atr3 != atr11] select " +
                               "atr1 as symbol,sum(atr2) as price" +
                               " insert into " + "OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,101.0, 500l,200l,102.0,75.7f,false,true,105});
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100,101.0, 501l,201l,103.0,76.7f,false,true,106});
        inputHandler.send(new Object[]{"IBM", 75.6f, 100,102.0, 502l,202l,104.0,77.7f,false,true,107});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100,101.0, 502l,202l,104.0,77.7f,false,true,108});
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        executionRuntime.shutdown();
    }
}

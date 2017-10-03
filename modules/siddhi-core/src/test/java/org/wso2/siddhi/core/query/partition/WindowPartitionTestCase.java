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

import java.util.concurrent.atomic.AtomicInteger;

public class WindowPartitionTestCase {
    static final Logger log = Logger.getLogger(WindowPartitionTestCase.class);
    private int inEventCount;
    private AtomicInteger inEventAtomicCount;
    private int removeEventCount;
    private boolean eventArrived;
    private boolean firstEvent;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        firstEvent = true;
        inEventAtomicCount = new AtomicInteger(0);
    }


    @Test
    public void testWindowPartitionQuery1() throws InterruptedException {
        log.info("Window Partition test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "define stream cseEventStream (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from cseEventStream#window.length(2)  select symbol,sum(price) as price,volume insert expired events into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    removeEventCount++;
                    if (removeEventCount == 1) {
                        Assert.assertEquals(100.0, event.getData()[1]);
                    } else if (removeEventCount == 2) {
                        Assert.assertEquals(1000.0, event.getData()[1]);
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 70f, 100});
        inputHandler.send(new Object[]{"WSO2", 700f, 100});
        inputHandler.send(new Object[]{"IBM", 100f, 100});
        inputHandler.send(new Object[]{"IBM", 200f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 1000f, 100});
        inputHandler.send(new Object[]{"WSO2", 500f, 100});

        Thread.sleep(1000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(2, removeEventCount);
        executionRuntime.shutdown();

    }

    @Test
    public void testWindowPartitionQuery2() throws InterruptedException {
        log.info("Window Partition test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "define stream cseEventStream (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from cseEventStream#window.lengthBatch(2)  select symbol,sum(price) as price,volume insert all events into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    inEventCount++;
                    eventArrived = true;
                    if (inEventCount == 1) {
                        Assert.assertEquals(170.0, event.getData()[1]);
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(1700.0, event.getData()[1]);
                    }
                }

            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 70f, 100});
        inputHandler.send(new Object[]{"WSO2", 700f, 100});
        inputHandler.send(new Object[]{"IBM", 100f, 100});
        inputHandler.send(new Object[]{"IBM", 200f, 100});
        inputHandler.send(new Object[]{"WSO2", 1000f, 100});

        Thread.sleep(2000);
        Assert.assertEquals(2, inEventCount);
        executionRuntime.shutdown();

    }

    @Test
    public void testWindowPartitionQuery3() throws InterruptedException {
        log.info("Window Partition test3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "define stream cseEventStream (symbol string, price float,volume int);" +
                "" +
                "partition with (symbol of cseEventStream) " +
                "begin " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec)  " +
                "select symbol, sum(price) as price,volume " +
                "insert all events into OutStockStream ;  " +
                "end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    if (event.getData()[0].equals("WSO2")) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(700.0, event.getData()[1]);
                        } else if (inEventCount == 2) {
                            Assert.assertEquals(0.0, event.getData()[1]);
                        } else if (inEventCount == 3) {
                            Assert.assertEquals(1000.0, event.getData()[1]);
                        } else if (inEventCount == 4) {
                            Assert.assertEquals(0.0, event.getData()[1]);
                        }
                    } else {
                        removeEventCount++;
                        if (removeEventCount == 1) {
                            Assert.assertEquals(70.0, event.getData()[1]);
                        } else if (removeEventCount == 2) {
                            Assert.assertEquals(170.0, event.getData()[1]);
                        } else if (removeEventCount == 3) {
                            Assert.assertEquals(100.0, event.getData()[1]);
                        } else if (removeEventCount == 4) {
                            Assert.assertEquals(0.0, event.getData()[1]);
                        } else if (removeEventCount == 5) {
                            Assert.assertEquals(200.0, event.getData()[1]);
                        } else if (removeEventCount == 6) {
                            Assert.assertEquals(0.0, event.getData()[1]);
                        }
                    }
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 70f, 100});
        inputHandler.send(new Object[]{"WSO2", 700f, 100});
        inputHandler.send(new Object[]{"IBM", 100f, 200});

        Thread.sleep(3000);
        inputHandler.send(new Object[]{"IBM", 200f, 300});
        inputHandler.send(new Object[]{"WSO2", 1000f, 100});

        Thread.sleep(2000);
        executionRuntime.shutdown();
        Assert.assertTrue(inEventCount == 4);
        Assert.assertTrue(removeEventCount == 6);
        Assert.assertTrue(eventArrived);


    }


    @Test
    public void testWindowPartitionQuery4() throws InterruptedException {
        log.info("Window Partition test4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "define stream cseEventStream (symbol string, price float,volume int);"
                + "partition with (symbol of cseEventStream) begin @info(name = 'query1') from cseEventStream#window.length(2)  select symbol,sum(price) as price,volume insert into OutStockStream ;  end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if (event.isExpired()) {
                        removeEventCount++;
                    } else {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(70.0, event.getData()[1]);
                        } else if (inEventCount == 2) {
                            Assert.assertEquals(700.0, event.getData()[1]);
                        } else if (inEventCount == 3) {
                            Assert.assertEquals(170.0, event.getData()[1]);
                        } else if (inEventCount == 4) {
                            Assert.assertEquals(300.0, event.getData()[1]);
                        } else if (inEventCount == 5) {
                            Assert.assertEquals(75.5999984741211, event.getData()[1]);
                        } else if (inEventCount == 6) {
                            Assert.assertEquals(1700.0, event.getData()[1]);
                        } else if (inEventCount == 7) {
                            Assert.assertEquals(1500.0, event.getData()[1]);
                        }
                    }


                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 70f, 100});
        inputHandler.send(new Object[]{"WSO2", 700f, 100});
        inputHandler.send(new Object[]{"IBM", 100f, 100});
        inputHandler.send(new Object[]{"IBM", 200f, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 1000f, 100});
        inputHandler.send(new Object[]{"WSO2", 500f, 100});

        Thread.sleep(1000);
        Assert.assertTrue(eventArrived);
        Assert.assertTrue(7 >= inEventCount);
        Assert.assertEquals(0, removeEventCount);
        executionRuntime.shutdown();

    }


    @Test
    public void testWindowPartitionQuery5() throws InterruptedException {
        log.info("Window Partition test5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "define stream cseEventStream (symbol string, price double,volume int);"
                + "" +
                "partition with (symbol of cseEventStream) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from cseEventStream#window.timeBatch(5 sec)  " +
                "   select symbol, sum(price) as price, volume " +
                "   insert into OutStockStream ;  " +
                "end ";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);


        executionRuntime.addCallback("OutStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if (event.isExpired()) {
                        removeEventCount++;
                    } else {
                        inEventCount++;
                        if ("IBM".equals(event.getData()[0])) {
                            Assert.assertEquals(370.0, event.getData()[1]);
                        } else if ("WSO2".equals(event.getData()[0])) {
                            Assert.assertEquals(2200.0, event.getData()[1]);
                        } else if ("ORACLE".equals(event.getData()[0])) {
                            Assert.assertEquals(75.6, event.getData()[1]);
                        }
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionRuntime.getInputHandler("cseEventStream");
        executionRuntime.start();
        inputHandler.send(new Object[]{"IBM", 70.0, 100});
        inputHandler.send(new Object[]{"WSO2", 700.0, 100});
        inputHandler.send(new Object[]{"IBM", 100.0, 100});
        inputHandler.send(new Object[]{"IBM", 200.0, 100});
        inputHandler.send(new Object[]{"ORACLE", 75.6, 100});
        inputHandler.send(new Object[]{"WSO2", 1000.0, 100});
        inputHandler.send(new Object[]{"WSO2", 500.0, 100});

        Thread.sleep(7000);
        Assert.assertTrue(eventArrived);
        Assert.assertTrue(7 >= inEventCount);
        Assert.assertEquals(0, removeEventCount);
        executionRuntime.shutdown();

    }

    @Test
    public void testWindowPartitionQuery6() throws InterruptedException {
        log.info("Window Partition test6 - TableWindow");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream order (billnum string, custid string, items string, dow string, timestamp long); " +
                "define table dow_items (custid string, dow string, item string) ; " +
                "define stream dow_items_stream (custid string, dow string, item string); ";
        String query = "" +
                "partition with (custid of order) " +
                "begin " +
                "@info(name = 'query1') " +
                "from order join dow_items \n" +
                "on order.custid == dow_items.custid \n" +
                "select  dow_items.item\n" +
                "having order.items == \"item1\" \n" +
                "insert into recommendationStream ;" +
                "end;" +
                "@info(name = 'query2') " +
                "from dow_items_stream " +
                "insert into dow_items ;" +
                "" +
                "";


        ExecutionPlanRuntime executionRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);


        executionRuntime.addCallback("recommendationStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if (event.isExpired()) {
                        removeEventCount++;
                    } else {
                        inEventAtomicCount.getAndIncrement();
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler1 = executionRuntime.getInputHandler("dow_items_stream");
        InputHandler inputHandler2 = executionRuntime.getInputHandler("order");
        executionRuntime.start();
        inputHandler1.send(new Object[]{"1","MON", "item1"});
        inputHandler2.send(new Object[]{"123","1", "item1", "MON", System.currentTimeMillis()});


        SiddhiTestHelper.waitForEvents(100, 1, inEventAtomicCount, 5000);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(1, inEventAtomicCount.intValue());
        executionRuntime.shutdown();

    }
    @Test
    public void testEventWindowPartition() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) lengthBatch"
                + "(4); ";

        String query = "partition with (symbol of cseEventStream, symbol of cseEventWindow)" +
                "begin " +
                "@info(name = 'query0') " +

                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') from cseEventWindow " +
                "select symbol,sum(price) as totalPrice,volume " +
                "group by symbol " +
                "insert into outputStream ;" +
                "end;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    if (!event.isExpired()) {
                        inEventAtomicCount.incrementAndGet();
                        //We are getting four events even after groupby because of partitioning. Partition receiver will
                        //break the coming event chunk. If we fix that to group all events for same key into one
                        // chunk then event order will get affected.
                        if (inEventAtomicCount.get() == 1) {
                            Assert.assertEquals(700.0, event.getData(1));
                            Assert.assertEquals("IBM", event.getData(0));
                        } else if (inEventAtomicCount.get() == 2) {
                            Assert.assertEquals(60.5, event.getData(1));
                            Assert.assertEquals("WSO2", event.getData(0));
                        } else if (inEventAtomicCount.get() == 3) {
                            Assert.assertEquals(1401.0, event.getData(1));
                            Assert.assertEquals("IBM", event.getData(0));
                        } else if (inEventAtomicCount.get() == 3) {
                            Assert.assertEquals(123.0, event.getData(1));
                            Assert.assertEquals("WSO2", event.getData(0));
                        }
                    }
                }
                eventArrived = true;
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"IBM", 701f, 1});
        inputHandler.send(new Object[]{"WSO2", 62.5f, 1});
        SiddhiTestHelper.waitForEvents(100, 4, inEventAtomicCount, 10000);
        Assert.assertEquals(4, inEventAtomicCount.intValue());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();

    }


}

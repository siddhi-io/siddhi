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
//// TODO: 9/26/17 implement after fixing partition for defined windows
//    @Test
//    public void testWindowPartitionQuery7() throws InterruptedException {
//        log.info("JoinWindowPartition Test7");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String streams = "" +
//                "define stream cseEventStream (symbol string, price float, volume int); " +
//                "define stream twitterStream (user string, tweet string, company string); " +
//                "define window cseEventWindow (symbol string, price float, volume int) timeBatch(1 sec); ";
//
//        String query = "" +
//                "@info(name = 'query0') " +
//                "from cseEventStream " +
//                "insert into cseEventWindow; " +
//                "" +
//                "partition with (company of twitterStream) " +
//                "begin " +
//                "@info(name = 'query2') " +
//                "from cseEventWindow join twitterStream#window.timeBatch(1 sec) " +
//                "on cseEventWindow.symbol== twitterStream.company " +
//                "select cseEventWindow.symbol as symbol, twitterStream.tweet, cseEventWindow.price " +
//                "insert all events into outputStream ;" +
//                "end";
//
//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
//        try {
//            executionPlanRuntime.addCallback("query2", new QueryCallback() {
//                @Override
//                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                    EventPrinter.print(timeStamp, inEvents, removeEvents);
//                    if (inEvents != null) {
//                        inEventCount += (inEvents.length);
//                    }
//                    if (removeEvents != null) {
//                        removeEventCount += (removeEvents.length);
//                    }
//                    eventArrived = true;
//                }
//            });
//            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
//            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
//            executionPlanRuntime.start();
//            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
//            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
//            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
//            Thread.sleep(1100);
//            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
//            Thread.sleep(1000);
//            Assert.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
//            Assert.assertTrue("Removed Events can be 1 or 2 ", removeEventCount == 1 || removeEventCount == 2);
//            Assert.assertTrue(eventArrived);
//        } finally {
//            executionPlanRuntime.shutdown();
//        }
//    }


}

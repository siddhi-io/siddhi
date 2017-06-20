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

package org.wso2.siddhi.core.query.executor;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.Random;

public class GlobalExpressionTestCase {
    private static final Logger log = Logger.getLogger(GlobalExpressionTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testExpressionDefinition1() throws InterruptedException {
        log.info("Testing expression definition 1");

//        SiddhiManager siddhiManager = new SiddhiManager();
//        String cseEventStream = "define expression exp1 volume >= 10;" +
//                "define stream threshold (value int); ";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream);
//
//        siddhiAppRuntime.shutdown();
    }

//    @Test
//    public void testExpressionDefinition2() throws InterruptedException {
//        log.info("Testing expression definition 2");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String cseEventStream = "define variable threshold int;" +
//                "define stream threshold (value int); ";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test(expected = SiddhiAppValidationException.class)
//    public void testExpressionDefinition3() throws InterruptedException {
//        log.info("Testing expression definition 3");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String cseEventStream = "define variable threshold int = true;" +
//                "define stream threshold (value int); ";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test
//    public void testExpressionDefinition4() throws InterruptedException {
//        log.info("Testing expression definition 4");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String cseEventStream = "define variable threshold object;" +
//                "define stream threshold (value int); ";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//    @Test(expected = SiddhiAppValidationException.class)
//    public void testExpressionDefinition5() throws InterruptedException {
//        log.info("Testing expression definition 5");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        String cseEventStream = "define variable threshold object = 5;" +
//                "define stream threshold (value int); ";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream);
//
//        siddhiAppRuntime.shutdown();
//    }

    @Test
    public void testExpressionReference1() throws InterruptedException {
        log.info("Testing expression reference as an attribute value");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define expression greaterThan price >= 50.0f;" +
                "define stream ThresholdStream (value int); " +
                "define stream cseEventStream (symbol string, price float, volume int); ";

        String query = "@info(name = 'query1') from cseEventStream[global::greaterThan] select symbol,price, volume " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount += inEvents.length;
                    if (inEventCount == 1) {
                        Assert.assertArrayEquals(new Object[]{"WSO2", 56.50f, 5}, inEvents[0].getData());
                    }
                }
                if (removeEvents != null) {
                    removeEventCount += removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHanlder = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler thresholdStreamHanlder = siddhiAppRuntime.getInputHandler("ThresholdStream");

        cseEventStreamHanlder.send(new Object[]{"WSO2", 56.50f, 5});
        thresholdStreamHanlder.send(new Object[]{100});
        cseEventStreamHanlder.send(new Object[]{"GOOGLE", 46.50f, 5});

        Thread.sleep(100);

        siddhiAppRuntime.shutdown();


        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("In Event count", 1, inEventCount);
        Assert.assertEquals("Out Event count", 0, removeEventCount);
    }

    @Test
    public void testExpressionReference2() throws InterruptedException {
        log.info("Testing expression reference in stream filtering");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define variable threshold int = 0;" +
                "define stream ThresholdStream (value int); " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream outputStream (symbol string, price float, volume int); ";

        String query = "@info(name = 'query1') from cseEventStream[global#threshold > 5] select symbol,price, " +
                "global#threshold as volume insert all events into outputStream; " +
                "@info(name = 'query2') from ThresholdStream select value update global#threshold; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount += inEvents.length;
                    if (inEventCount == 1) {
                        Assert.assertArrayEquals(new Object[]{"GOOGLE", 56.50f, 10}, inEvents[0].getData());
                    }
                }
                if (removeEvents != null) {
                    removeEventCount += removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler cseEventStreamHanlder = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler thresholdStreamHanlder = siddhiAppRuntime.getInputHandler("ThresholdStream");

        cseEventStreamHanlder.send(new Object[]{"WSO2", 56.50f, 5});
        thresholdStreamHanlder.send(new Object[]{10});
        cseEventStreamHanlder.send(new Object[]{"GOOGLE", 56.50f, 5});

        Thread.sleep(100);

        siddhiAppRuntime.shutdown();


        Assert.assertEquals("Event arrived", true, eventArrived);
        Assert.assertEquals("In Event count", 1, inEventCount);
        Assert.assertEquals("Out Event count", 0, removeEventCount);
    }

    @Test
    public void testExpressionReference3() throws InterruptedException {
        log.info("Testing expression reference as the length of a LengthWindow");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define variable length int = 2;" +
                "define stream lengthStream (value int); " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.length(global#length) select symbol,price," +
                "volume insert all events into outputStream; " +
                "@info(name = 'query2') from lengthStream select value update global#length; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertEquals("Message order inEventCount", inEventCount, inEvents[0].getData(2));
                Assert.assertEquals("Events cannot be expired", false, inEvents[0].isExpired());
                inEventCount = inEventCount + inEvents.length;
                if (removeEvents != null) {
                    removeEventCount += removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler lengthHandler = siddhiAppRuntime.getInputHandler("lengthStream");

        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"ORACLE", 80.0f, 2});

        lengthHandler.send(new Object[]{1});

        inputHandler.send(new Object[]{"GOOGLE", 90.0f, 3});

        Thread.sleep(500);
        Assert.assertEquals(4, inEventCount);
        Assert.assertEquals(3, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testExpressionReference4() throws InterruptedException {
        log.info("Testing expression reference as the length of a LengthBatchWindow");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define variable length int = 4;" +
                "define stream lengthStream (value int); " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseWindow (symbol string, price float, volume int) lengthBatch(global#length); ";
        String query = "@info(name = 'query1') from cseEventStream select symbol,price,volume insert into cseWindow ;" +
                "@info(name = 'query2') from cseWindow insert into outputStream ; " +
                "@info(name = 'query3') from lengthStream select value update global#length; ";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    Assert.assertEquals("In event order", count, event.getData(2));
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler lengthHandler = siddhiAppRuntime.getInputHandler("lengthStream");

        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});  // Expires 4 events

        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});

        lengthHandler.send(new Object[]{2});    // Update the window length
        inputHandler.send(new Object[]{"IBM", 700f, 7});    // Expires 2 + 1 = 3 events

        inputHandler.send(new Object[]{"WSO2", 60.5f, 8});
        inputHandler.send(new Object[]{"IBM", 700f, 9});     // Expires 2 events

        Thread.sleep(500);
        Assert.assertEquals("Total event count", 9, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExpressionReference5() throws InterruptedException {
        log.info("Testing expression reference as the time of a TimeWindow");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define variable time long = 1 sec;" +
                "define stream timeStream (value long); " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream outputStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.time(global#time) select symbol,price," +
                "volume insert all events into outputStream ; " +
                "@info(name = 'query2') from timeStream select value update global#time; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler timeHandler = siddhiAppRuntime.getInputHandler("timeStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        Thread.sleep(1100);  // Expires 2 events
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});

        timeHandler.send(new Object[]{500L});
        Thread.sleep(510);  // Expires 2 events
        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        Thread.sleep(510);  // Expires 2 events
        Assert.assertEquals(6, inEventCount);
        Assert.assertEquals(6, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExpressionReference6() throws InterruptedException {
        log.info("Testing expression reference as the time of a TimeBatchWindow");

        SiddhiManager siddhiManager = new SiddhiManager();


        String cseEventStream = "define variable time long = 1 sec;" +
                "define stream timeStream (value long); " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(global#time) " +
                "select symbol, sum(price) as price " +
                "insert all events into outputStream; " +
                "@info(name = 'query2') from timeStream select value update global#time; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler timeHandler = siddhiAppRuntime.getInputHandler("timeStream");

        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});

        timeHandler.send(new Object[]{500L});
        Thread.sleep(505);

        inputHandler.send(new Object[]{"IBM", 700f, 5});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 6});
        Thread.sleep(1000);
        Assert.assertEquals(3, inEventCount);
        Assert.assertEquals(1, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExpressionReference7() throws InterruptedException {
        log.info("Testing timeLength window with no of events greater than window length and time period less than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String sensorStream = "define variable length int = 4;" +
                "define variable time long = 10 sec;" +
                "define stream lengthStream (value int); " +
                "define stream timeStream (value long); " +
                "define stream sensorStream (id string, sensorValue float);";

        String query = "@info(name = 'query1') from sensorStream#window.timeLength(global#time, global#length)" +
                " select id,sensorValue" +
                " insert all events into outputStream; " +
                "" +
                "@info(name = 'query2') from timeStream select value update global#time; " +
                "" +
                "@info(name = 'query3') from lengthStream select value update global#length; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sensorStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("sensorStream");
        InputHandler lengthHandler = siddhiAppRuntime.getInputHandler("lengthStream");
        InputHandler timeHandler = siddhiAppRuntime.getInputHandler("timeStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"id1", 10d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id2", 20d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id3", 30d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id4", 40d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id5", 50d});

        lengthHandler.send(new Object[]{2});

        Thread.sleep(100);
        inputHandler.send(new Object[]{"id6", 60d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id7", 70d});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id8", 80d});

        Thread.sleep(2000);

        Assert.assertEquals(8, inEventCount);
        Assert.assertEquals(4, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExpressionReference8() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "define variable time long = 5 sec;" +
                "define stream inputStream(currentTime long,value int); " +
                "define stream timeStream (value long); ";
        String query = " " +
                "@info(name='query') " +
                "from inputStream#window.externalTimeBatch(currentTime, global#time) " +
                "select value " +
                "insert into outputStream; " +
                "" +
                "@info(name = 'query2') from timeStream select value update global#time; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + query);
        siddhiAppRuntime.addCallback("query", new QueryCallback() {
            int count = 0;

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count += 1;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        InputHandler timeHandler = siddhiAppRuntime.getInputHandler("timeStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{10000L, 1});
        Thread.sleep(100);
        inputHandler.send(new Object[]{11000L, 2});
        Thread.sleep(100);
        inputHandler.send(new Object[]{12000L, 3});
        Thread.sleep(100);
        timeHandler.send(new Object[]{2000L});  // Change the scheduler time
        inputHandler.send(new Object[]{13000L, 4});
        Thread.sleep(100);
        inputHandler.send(new Object[]{14000L, 5});
        Thread.sleep(100);
        timeHandler.send(new Object[]{2000L});  // Change the scheduler time
        Thread.sleep(100);
        inputHandler.send(new Object[]{15000L, 6});
        Thread.sleep(100);
        inputHandler.send(new Object[]{16500L, 7});
        Thread.sleep(100);
        inputHandler.send(new Object[]{17000L, 8});
        Thread.sleep(100);
        inputHandler.send(new Object[]{18000L, 9});
        Thread.sleep(100);
        inputHandler.send(new Object[]{19000L, 10});
        Thread.sleep(100);
        inputHandler.send(new Object[]{20000L, 11});
        Thread.sleep(100);
        inputHandler.send(new Object[]{20500L, 12});
        Thread.sleep(100);
        inputHandler.send(new Object[]{22000L, 13});
        Thread.sleep(100);
        inputHandler.send(new Object[]{23000L, 14});
        Thread.sleep(100);
    }


    //    @Test
    public void testPerformance() throws InterruptedException {
        for (int i = 50_000; i <= 1_000_000; i += 50_000) {
            test(i);
        }

    }

    private void test(int length) throws InterruptedException {
        Random random = new Random();

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define variable length int = 2;" +
                "define stream lengthStream (value int); " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseWindow (symbol string, price float, volume int) lengthBatch(global#length); ";
        String query = "@info(name = 'query1') from cseEventStream select symbol,price,volume insert into cseWindow ;" +
                "@info(name = 'query2') from cseWindow insert into outputStream ; " +
                "@info(name = 'query3') from lengthStream select value update global#length; ";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler lengthHandler = siddhiAppRuntime.getInputHandler("lengthStream");

        siddhiAppRuntime.start();

        long startTime = System.currentTimeMillis();
        int windowLength = 2;
        for (int i = 0; i < length; i++) {
            if (i % 1000 == 0) {
                windowLength++;
                lengthHandler.send(new Object[]{windowLength});    // Update the window length
            }
            inputHandler.send(new Object[]{"WSO2", (random.nextFloat() * 100), 1});

        }
        long endTime = System.currentTimeMillis();

        Thread.sleep(500);
        siddhiAppRuntime.shutdown();

        double avgTime = (double) ((endTime - startTime)) / length;
        System.out.printf("%d, %.8f\n", length, avgTime);
    }
}

/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.query.window.external;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class TimeLengthWindowTestCase {
    private static final Logger log = Logger.getLogger(TimeLengthWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private int count = 0;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    /*
        Time Period < Window time
        Number of Events < Window length
    */
    @Test
    public void testTimeLengthWindow1() throws InterruptedException {
        log.info("Testing timeLength window with no of events less than window length and time period less than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) timeLength(4 sec,10); ";

        String query = "@info(name = 'query0') from cseEventStream insert into cseEventWindow; @info(name = 'query1')" +
                " from cseEventWindow select symbol,price," +
                "volume insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
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
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 700f, 3});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 4});
        Thread.sleep(5000);

        Assert.assertEquals(4, inEventCount);
        Assert.assertEquals(4, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    /*
       Time Period > Window time
       Number of Events < Window length
    */
    @Test
    public void testTimeLengthWindow2() throws InterruptedException {
        log.info("Testing timeLength window with no of events less than window length and time period greater than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) timeLength(2 sec,10); ";

        String query = "@info(name = 'query0') from cseEventStream insert into cseEventWindow; " +
                "@info(name = 'query1') from cseEventWindow select symbol,price, volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
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
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        Thread.sleep(1200);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(1200);
        inputHandler.send(new Object[]{"Google", 80.5f, 2});
        Thread.sleep(1200);
        inputHandler.send(new Object[]{"Yahoo", 90.5f, 3});
        Thread.sleep(4000);
        Assert.assertEquals(4, inEventCount);
        Assert.assertEquals(4, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    /*
        Time Period < Window time
        Number of Events > Window length
    */
    @Test
    public void testTimeLengthWindow3() throws InterruptedException {
        log.info("Testing timeLength window with no of events greater than window length and time period less than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String sensorStream = "define stream sensorStream (id string, sensorValue float); " +
                "define window sensorWindow (id string, sensorValue float) timeLength(10 sec,4); ";
        String query = "@info(name = 'query0') from sensorStream " +
                "insert into sensorWindow; " +
                "@info(name = 'query1') from sensorWindow " +
                " select id,sensorValue" +
                " insert all events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sensorStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
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
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"id1", 10d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id2", 20d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id3", 30d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id4", 40d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id5", 50d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id6", 60d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id7", 70d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id8", 80d});

        Thread.sleep(2000);

        Assert.assertEquals(8, inEventCount);
        Assert.assertEquals(4, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    /*
           Time Period > Window time
           Number of Events > Window length
    */
    @Test
    public void testTimeLengthWindow4() throws InterruptedException {

        log.info("Testing timeLength window with no of events greater than window length and time period greater than" +
                " window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String sensorStream = "define stream sensorStream (id string, sensorValue float); " +
                "define window sensorWindow (id string, sensorValue float) timeLength(2 sec,4); ";
        String query = "@info(name = 'query0') from sensorStream " +
                "insert into sensorWindow; " +
                "@info(name = 'query1') from sensorWindow " +
                " select id,sensorValue" +
                " insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sensorStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
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
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"id1", 10d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id2", 20d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id3", 30d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id4", 40d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id5", 50d});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"id6", 60d});

        Thread.sleep(2100);

        Assert.assertEquals(6, inEventCount);
        Assert.assertEquals(6, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    /*
        Time period > Window time
        Number of events > length
    */
    @Test
    public void testTimeLengthWindow6() throws InterruptedException {
        log.info("Testing timeLength window with no of events greater than window length and time period greater than" +
                " window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String sensorStream = "define stream sensorStream (id string, sensorValue int); " +
                "define window sensorWindow (id string, sensorValue int) timeLength(3 sec,6); ";
        String query = "@info(name = 'query0') from sensorStream " +
                "insert into sensorWindow; " +
                "@info(name = 'query1') from sensorWindow " +
                " select id,sum(sensorValue) as sum" +
                " insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sensorStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);

                if (inEvents != null) {
                    if (inEvents[0].getData(0).toString().equals("id6")) {
                        Assert.assertEquals("6", inEvents[0].getData(1).toString());
                    }
                    if (inEvents[0].getData(0).toString().equals("id7")) {
                        Assert.assertEquals("6", inEvents[0].getData(1).toString());
                    }
                    if (inEvents[0].getData(0).toString().equals("id8")) {
                        Assert.assertEquals("6", inEvents[0].getData(1).toString());
                    }
                    inEventCount++;
                }

                if (removeEvents != null) {
                    if (removeEvents[0].getData(0).toString().equals("id1")) {
                        Assert.assertEquals("5", removeEvents[0].getData(1).toString());
                    }
                    if (removeEvents[0].getData(0).toString().equals("id2")) {
                        Assert.assertEquals("5", removeEvents[0].getData(1).toString());
                    }
                    if (removeEvents[0].getData(0).toString().equals("id3")) {
                        Assert.assertEquals("3", removeEvents[0].getData(1).toString());
                    }
                    removeEventCount++;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("sensorStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"id1", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id2", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id3", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id4", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id5", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id6", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id7", 1});
        Thread.sleep(520);
        inputHandler.send(new Object[]{"id8", 1});

        Thread.sleep(1000);

        Assert.assertEquals(8, inEventCount);
        Assert.assertEquals(2, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    /*
        Time period < Window time
        Number of events < length
    */
    @Test
    public void testTimeLengthWindow7() throws InterruptedException {
        log.info("Testing timeLength window with no of events less than window length and time period less than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String sensorStream = "define stream sensorStream (id string, sensorValue int); " +
                "define window sensorWindow (id string, sensorValue int) timeLength(5 sec,5); ";
        String query = "@info(name = 'query0') from sensorStream " +
                "insert into sensorWindow; " +
                "@info(name = 'query1') from sensorWindow " +
                " select id,sum(sensorValue) as sum" +
                " insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sensorStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                Assert.assertEquals((long) count + 1, (inEvents[0].getData(1)));

                count++;
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("sensorStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"id1", 1});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id2", 1});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id3", 1});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"id4", 1});

        Thread.sleep(1000);

        Assert.assertEquals(4, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }


    /*
       Time Period < Window time
       Number of Events > Window length
    */

    @Test
    public void testTimeLengthWindow10() throws InterruptedException {
        log.info("Testing timeLength window with no of events greater than window length and time period less than " +
                "window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int); " +
                "define window cseEventWindow (symbol string, price float, volume int) timeLength(10 sec,5); ";
        String query = "@info(name = 'query0') from cseEventStream " +
                "insert into cseEventWindow; " +
                "@info(name = 'query1') from cseEventWindow " +
                " select symbol,volume" +
                " insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        if (event.isExpired()) {
                            removeEventCount++;
                        } else {
                            inEventCount++;
                        }
                    }
                }
                if (removeEvents != null) {
                    for (Event event : removeEvents) {
                        if (event.isExpired()) {
                            removeEventCount++;
                        } else {
                            inEventCount++;
                        }
                    }
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 10});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 20});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 700f, 20});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 40});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 700f, 50});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 60});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"IBM", 700f, 70});
        Thread.sleep(500);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 80});
        Thread.sleep(5000);
        Assert.assertEquals("In event count", 8, inEventCount);
        Assert.assertEquals("Remove event count", 3, removeEventCount);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

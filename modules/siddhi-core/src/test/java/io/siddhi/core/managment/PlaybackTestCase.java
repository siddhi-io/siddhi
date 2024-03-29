/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.managment;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PlaybackTestCase {
    private static final Logger log = LogManager.getLogger(PlaybackTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void playbackTest1() throws InterruptedException {
        log.info("Playback Test 1: Playback with heartbeat disabled in query containing regular time batch window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEventCount == 0) {
                    AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ", removeEvents
                            == null);
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});

        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});

        timestamp += 500;   // 1 sec passed
        inputHandler.send(timestamp, new Object[]{"GOOGLE", 85.0f, 1});

        timestamp += 1000;   // Another 1 sec passed
        inputHandler.send(timestamp, new Object[]{"ORACLE", 90.5f, 1});
        Thread.sleep(100);

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"playbackTest1"})
    public void playbackTest2() throws InterruptedException {
        log.info("Playback Test 2: Playback with heartbeat disabled in query with start time enabled time batch " +
                "window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(2 sec , 0) " +
                "select symbol, sum(price) as sumPrice, volume " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEventCount == 0) {
                    AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ", removeEvents
                            == null);
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                } else if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        // Start sending events in the beginning of a cycle
        long timestamp = 0;
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        timestamp += 8500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(timestamp, new Object[]{"II", 60.5f, 1});
        timestamp += 13000;
        inputHandler.send(timestamp, new Object[]{"TT", 60.5f, 1});
        inputHandler.send(timestamp, new Object[]{"YY", 60.5f, 1});
        timestamp += 5000;
        inputHandler.send(timestamp, new Object[]{"ZZ", 0.0f, 0});
        Thread.sleep(100);

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 0, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"playbackTest2"})
    public void playbackTest3() throws InterruptedException {
        log.info("Playback Test 3: Playback with heartbeat enabled");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '2 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(2 sec , 0) " +
                "select symbol, sum(price) as sumPrice, volume " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEventCount == 0) {
                    AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ", removeEvents
                            == null);
                }
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                } else if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        // Start sending events in the beginning of a cycle
        long timestamp = 0;
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        timestamp += 8500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(timestamp, new Object[]{"II", 60.5f, 1});
        timestamp += 13000;
        inputHandler.send(timestamp, new Object[]{"TT", 60.5f, 1});
        inputHandler.send(timestamp, new Object[]{"YY", 60.5f, 1});
        Thread.sleep(200);  // Anything more than 100 is enough. Used 200 to be on safe side

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 0, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"playbackTest3"})
    public void playbackTest4() throws InterruptedException {
        log.info("Playback Test 4: Playback with query joining two windows");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '1 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) join twitterStream#window.timeBatch(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            long currentTime = System.currentTimeMillis();
            cseEventStreamHandler.send(currentTime, new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(currentTime, new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(currentTime, new Object[]{"IBM", 75.6f, 100});
            currentTime += 1500;
            cseEventStreamHandler.send(currentTime, new Object[]{"WSO2", 57.6f, 100});

            Thread.sleep(200);  // Anything more than 100 is enough. Used 200 to be on safe side

            AssertJUnit.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"playbackTest4"})
    public void playbackTest5() throws InterruptedException {
        log.info("Playback Test 5: Playback enabled timeLength window with no of events less than window length and " +
                "time period less than window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@app:playback define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.timeLength(4 sec,10) select symbol,price," +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 1});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 2});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 3});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 4});
        timestamp += 5000;
        inputHandler.send(timestamp, new Object[]{"GOOGLE", 90.5f, 5});

        SiddhiTestHelper.waitForEvents(100, 5, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 4, removeEventCount, 60000);

        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest5"})
    public void playbackTest6() throws InterruptedException {
        log.info("Playback Test 6: Playback with heartbeat enabled timeLength window with no of events less than " +
                "window length and time period less than window time");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@app:playback(idle.time = '100 millisecond', increment = '4 sec') define stream " +
                "cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.timeLength(4 sec,10) select symbol,price," +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 1});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 2});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 3});
        timestamp += 500;
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 4});
        Thread.sleep(200);  // Anything more than 100 is enough. Used 200 to be on safe side

        SiddhiTestHelper.waitForEvents(100, 4, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 4, removeEventCount, 60000);

        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest6"})
    public void playbackTest7() throws InterruptedException {
        log.info("Playback Test 7: Testing playback without heartbeat");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        timestamp += 2000;
        inputHandler.send(timestamp, new Object[]{"GOOGLE", 0.0f, 1});
        Thread.sleep(100);

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest7"})
    public void playbackTest8() throws InterruptedException {
        log.info("Playback Test 8: Testing playback with heartbeat enabled");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '2 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long timestamp = System.currentTimeMillis();
        inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
        inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
        Thread.sleep(200);  // Anything more than 100 is enough. Used 200 to be on safe side

        SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest8"}, expectedExceptions = SiddhiParserException.class)
    public void playbackTest9() throws InterruptedException {
        log.info("Playback Test 9: Testing playback with invalid increment time constant");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '2') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(dependsOnMethods = {"playbackTest9"}, expectedExceptions = SiddhiParserException.class)
    public void playbackTest10() throws InterruptedException {
        log.info("Playback Test 10: Testing playback with invalid idle.time time constant");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '', increment = '2 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(dependsOnMethods = {"playbackTest10"})
    public void playbackTest11() throws InterruptedException {
        log.info("Playback Test 11: Testing playback with out of order event with less than system timestamp");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '1 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(2 sec) " +
                "select symbol,price,volume " +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                    if (removeEventCount == 3) {
                        // Last timestamp is 200 + 4 sec (increment) = 2200
                        AssertJUnit.assertEquals(4200, removeEvents[0].getTimestamp());
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(100, new Object[]{"IBM", 700f, 0});
        inputHandler.send(200, new Object[]{"WSO2", 600.5f, 1});
        Thread.sleep(150);
        inputHandler.send(1150, new Object[]{"ORACLE", 500.0f, 2});  // Does no increase the system clock
        Thread.sleep(350);  // Anything more than 100 is enough. Used 200 to be on safe side

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 3, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest11"})
    public void playbackTest12() throws InterruptedException {
        log.info("Playback Test 12: Testing playback with out of order event with greater than system timestamp");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '1 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(2 sec) " +
                "select symbol,price,volume " +
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
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                    if (removeEventCount == 3) {
                        // Last timestamp is 1900 + 3 sec (increment) = 2200
                        AssertJUnit.assertEquals(4900, removeEvents[0].getTimestamp());
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(100, new Object[]{"IBM", 700f, 0});
        inputHandler.send(200, new Object[]{"WSO2", 600.5f, 1});
        Thread.sleep(150);
        inputHandler.send(1900, new Object[]{"ORACLE", 500.0f, 2});
        Thread.sleep(350);  // Anything more than 100 is enough. Used 200 to be on safe side

        SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
        SiddhiTestHelper.waitForEvents(100, 3, removeEventCount, 60000);

        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"playbackTest12"})
    public void playbackTest13() throws InterruptedException {
        log.info("Playback Test 13: Testing playback with out of order event with smaller than system timestamp after" +
                " window expires");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@app:playback(idle.time = '100 millisecond', increment = '1 sec') " +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(2 sec) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        AssertJUnit.assertTrue("InEvents arrived before RemoveEvents",
                                inEventCount > removeEventCount);
                        removeEventCount = removeEventCount + removeEvents.length;
                        if (removeEventCount == 3) {
                            // Last timestamp is 200 + 3 * 2000 (increment) = 6200
                            AssertJUnit.assertEquals(6200, removeEvents[0].getTimestamp());
                        }
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(100, new Object[]{"IBM", 700f, 0});
            inputHandler.send(200, new Object[]{"WSO2", 600.5f, 1});
            Thread.sleep(220);
            inputHandler.send(250, new Object[]{"ORACLE", 500.0f, 2});
            Thread.sleep(450);

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 3, removeEventCount, 60000);

            AssertJUnit.assertEquals(3, inEventCount);
            AssertJUnit.assertEquals(3, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"playbackTest13"})
    public void playbackTest14() throws InterruptedException {
        log.info("Playback Test 14: Switching to Playback mode in runtime with heartbeat disabled in query " +
                "containing regular time batch window ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents
                                        == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            inputHandler.send(timestamp, new Object[]{"IBM", 700f, 0});
            timestamp += 500;
            inputHandler.send(timestamp, new Object[]{"WSO2", 60.5f, 1});
            timestamp += 500;   // 1 sec passed
            inputHandler.send(timestamp, new Object[]{"GOOGLE", 85.0f, 1});
            timestamp += 1000;   // Another 1 sec passed
            inputHandler.send(timestamp, new Object[]{"ORACLE", 90.5f, 1});
            Thread.sleep(100);

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

            AssertJUnit.assertEquals(3, inEventCount);
            AssertJUnit.assertEquals(2, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }

    }

    @Test(dependsOnMethods = {"playbackTest14"})
    public void playbackTest15() throws InterruptedException {
        log.info("Playback Test 15: Switching between live mode and Playback mode with heartbeat disabled in query " +
                "containing regular time batch window ");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents
                                        == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 0});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            timestamp += 500;   // 1 sec passed
            inputHandler.send(timestamp, new Object[]{"GOOGLE", 85.0f, 1});
            timestamp += 1000;   // Another 1 sec passed
            inputHandler.send(timestamp, new Object[]{"ORACLE", 90.5f, 1});
            Thread.sleep(100);

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

            AssertJUnit.assertEquals(3, inEventCount);
            AssertJUnit.assertEquals(2, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }

    }

    @Test(dependsOnMethods = {"playbackTest15"})
    public void playbackTest16() throws InterruptedException {

        log.info("Playback Test 16: Switching between Playback mode and live mode with heartbeat disabled in query " +
                "containing regular time batch window test both current and expired batches");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents
                                        == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            inputHandler.send(timestamp - 500, new Object[]{"IBM", 700f, 0});
            inputHandler.send(timestamp - 100, new Object[]{"WSO2", 60.5f, 1});
            siddhiAppRuntime.enablePlayBack(false, null, null);
            Thread.sleep(1000);   // 1 sec passed
            inputHandler.send(new Object[]{"GOOGLE", 85.0f, 1});
            Thread.sleep(1000);   // Another 1 sec passed
            inputHandler.send(new Object[]{"ORACLE", 10000.5f, 1});
            Thread.sleep(100);

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 2, removeEventCount, 60000);

            AssertJUnit.assertEquals(3, inEventCount);
            AssertJUnit.assertEquals(2, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"playbackTest16"})
    public void playbackTest17() throws InterruptedException {
        log.info("Playback Test 17: Switching between Playback mode and live mode with heartbeat disabled in query " +
                "containing regular time batch window test only current batch ");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents
                                        == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            Event[] events = new Event[]{new Event(timestamp - 500, new Object[]{"IBM", 700f, 0}),
                    new Event(timestamp - 300, new Object[]{"WSO2", 60.5f, 1})};
            inputHandler.send(events);
            siddhiAppRuntime.enablePlayBack(false, null, null);
            Thread.sleep(805);   // 1 sec passed

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 0, removeEventCount, 60000);

            AssertJUnit.assertEquals(2, inEventCount);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"playbackTest17"})
    public void playbackTest17_1() throws InterruptedException {
        log.info("Playback Test 17: Switching between Playback mode and live mode with heartbeat disabled in query " +
                "containing regular time batch window test only current batch ");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents
                                        == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            inputHandler.send(timestamp - 500, new Object[]{"IBM", 700f, 0});
            Thread.sleep(10);
            inputHandler.send(timestamp - 100, new Object[]{"WSO2", 60.5f, 1});
            siddhiAppRuntime.enablePlayBack(false, null, null);
            Thread.sleep(605);   // 1 sec passed

            SiddhiTestHelper.waitForEvents(100, 2, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 0, removeEventCount, 60000);

            AssertJUnit.assertEquals(2, inEventCount);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"playbackTest17_1"})
    public void playbackTest18() throws InterruptedException {
        log.info("Playback Test 18: Switching between Playback mode and live mode with heartbeat disabled in query " +
                "containing regular time batch window test only current batch ");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    if (inEventCount == 0) {
                        AssertJUnit.assertTrue("Remove Events will only arrive after the second time period. ",
                                removeEvents == null);
                    }
                    if (inEvents != null) {
                        inEventCount = inEventCount + inEvents.length;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            siddhiAppRuntime.enablePlayBack(true, null, null);
            long timestamp = System.currentTimeMillis();
            inputHandler.send(timestamp - 500, new Object[]{"IBM", 700f, 0});
            Thread.sleep(10);
            inputHandler.send(timestamp - 100, new Object[]{"WSO2", 60.5f, 1});
            siddhiAppRuntime.enablePlayBack(false, null, null);
            inputHandler.send(System.currentTimeMillis(), new Object[]{"ORACLE", 60.5f, 1});
            Thread.sleep(505);   // 1 sec passed

            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 60000);
            SiddhiTestHelper.waitForEvents(100, 0, removeEventCount, 60000);
            AssertJUnit.assertEquals(3, inEventCount);
            AssertJUnit.assertEquals(0, removeEventCount);
            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}


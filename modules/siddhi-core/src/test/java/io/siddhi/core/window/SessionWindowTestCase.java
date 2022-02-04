/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.window;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.UnitTestAppender;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.query.processor.stream.window.SessionWindowProcessor;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Session window test case implementation.
 */
public class SessionWindowTestCase {

    private static final Logger log = (Logger) LogManager.getLogger(SessionWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private boolean innerAssertionsPassed;
    private AtomicInteger count = new AtomicInteger(0);
    private long eventTimeStamp;
    private double averageValue;
    private double totalValue;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        innerAssertionsPassed = false;
        eventTimeStamp = 0;
        count.set(0);
        averageValue = 0.0;
        totalValue = 0.0;
    }

    @Test(description = "This test checks if Siddhi App creation fails when more than three parameters are provided'",
            expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow1() {
        log.info("SessionWindow Test1: Testing session window with more than defined parameters");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, user, 2 sec, 1) "
                + "select * "
                + "insert all events into outputStream ;";

        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute types " +
                    "'<LONG, STRING, LONG, INT>'. Supported parameter overloads are " +
                    "(<INT|LONG|TIME> session.gap), " +
                    "(<INT|LONG|TIME> session.gap, <STRING> session.key), (<INT|LONG|TIME> session.gap, " +
                    "<STRING> session.key, <INT|LONG|TIME> allowed.latency).", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test(description = "This test checks if Siddhi App creation fails when session gap parameter is dynamic",
            expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow2() {
        log.info("SessionWindow Test2: Testing session window with providing session gap parameter as a dynamic one");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(item_number, user, 2 sec) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("The 'session' expects input parameter 'session.gap' at position '0' " +
                    "to be static, but found a dynamic attribute.", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails "
            + "when session gap parameter with wrong data type ", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow3() {
        log.info("SessionWindow Test3: Testing session window "
                + "with providing wrong data type for session gap parameter ");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session('3', user, 2 sec) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute types " +
                    "'<STRING, STRING, LONG>'. Supported parameter overloads are (<INT|LONG|TIME> session.gap), " +
                    "(<INT|LONG|TIME> session.gap, <STRING> session.key), (<INT|LONG|TIME> session.gap, " +
                    "<STRING> session.key, <INT|LONG|TIME> allowed.latency).", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails "
            + "when session key parameter with a constant value", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow4() {
        log.info("SessionWindow Test4: Testing session window "
                + "with providing a constant value for session key parameter ");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, 'user', 2 sec) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("Session window's 2nd parameter, session key should be a " +
                    "dynamic parameter attribute but found a constant attribute " +
                    "io.siddhi.core.executor.ConstantExpressionExecutor", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails "
            + "when session key parameter with wrong data type", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow5() {
        log.info("SessionWindow Test5: Testing session window "
                + "with defining wrong data type for session key parameter ");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, item_number, 2 sec) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute types " +
                    "'<LONG, INT, LONG>'. Supported parameter overloads are (<INT|LONG|TIME> session.gap), " +
                    "(<INT|LONG|TIME> session.gap, <STRING> session.key), (<INT|LONG|TIME> session.gap, " +
                    "<STRING> session.key, <INT|LONG|TIME> allowed.latency).", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails when"
            + " allowedLatency parameter with a dynamic value", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow6() {
        log.info("SessionWindow Test6: Testing session window "
                + "with providing dynamic value for allowedLatency");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, user, item_number) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("The 'session' expects input parameter 'allowed.latency' at position " +
                    "'2' to be static, but found a dynamic attribute.", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails when"
            + " allowedLatency parameter with a wrong data type", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow7() {
        log.info("SessionWindow Test7: Testing session window "
                + "with providing wrong data type for allowedLatency");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, user, '4') "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute " +
                            "types '<LONG, STRING, STRING>'. Supported parameter overloads are " +
                            "(<INT|LONG|TIME> session.gap), " +
                            "(<INT|LONG|TIME> session.gap, <STRING> session.key), " +
                            "(<INT|LONG|TIME> session.gap, <STRING> session.key, " +
                            "<INT|LONG|TIME> allowed.latency).",
                    e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails when"
            + " provided 2 paramters and 2nd parameter with wrong data type",
            expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow8() {
        log.info("SessionWindow Test8: Testing session window "
                + "with providing 2 parameters and 2nd parameter type is wrong");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, item_number) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute types" +
                    " '<LONG, INT>'. Supported parameter overloads are (<INT|LONG|TIME> session.gap), " +
                    "(<INT|LONG|TIME> session.gap, <STRING> session.key), (<INT|LONG|TIME> session.gap, " +
                    "<STRING> session.key, <INT|LONG|TIME> allowed.latency).", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks if Siddhi App creation fails when"
            + " provided 2 paramters and 2nd parameter allowedLatency with wrong data type",
            expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow9() {
        log.info("SessionWindow Test9: Testing session window "
                + "with providing 2 parameters and 2nd parameter allowedLatency type is wrong");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, '5') "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("Session window's allowedLatency parameter should be either int or long,"
                    + " but found STRING", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test checks whether the allowedLatency parameter value is greater"
            + " than the session gap value", expectedExceptions = SiddhiAppCreationException.class)
    public void testSessionWindow10() {
        log.info("SessionWindow Test10: Testing session window "
                + "with providing greater allowedLatency value than session gap value");

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int);";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(5 sec, 6 sec) "
                + "select * "
                + "insert all events into outputStream ;";
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("There is no parameterOverload for 'session' that matches attribute types " +
                    "'<LONG, LONG>'. Supported parameter overloads are (<INT|LONG|TIME> session.gap), " +
                    "(<INT|LONG|TIME> session.gap, <STRING> session.key), (<INT|LONG|TIME> session.gap, " +
                    "<STRING> session.key, <INT|LONG|TIME> allowed.latency).", e.getCause().getMessage());
            throw e;
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(description = "This test case checks an event initiate a session and wait for session timeout")
    public void testSessionWindow11() throws InterruptedException {
        log.info("SessionWindow Test11: Testing session window with input events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(1 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);
                }

                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 2) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(101, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send(new Object[]{"user0", 102, 24.6, 2});

        SiddhiTestHelper.waitForEvents(100, 4, count, 4000);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case checks two sessions are processed which  belong to the same session key")
    public void testSessionWindow12() throws InterruptedException {
        log.info("SessionWindow Test12: Testing session window, providing events "
                + "which are in 2 sessions with same session key");

        SiddhiManager siddhiManager = new SiddhiManager();

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(1 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 2) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(101, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    } else if (removeEventCount == 4) {
                        innerAssertionsPassed = false;
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(103, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(104, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send(new Object[]{"user0", 102, 24.5, 2});
        Thread.sleep(1500);

        inputHandler.send(new Object[]{"user0", 103, 22.4, 1});
        inputHandler.send(new Object[]{"user0", 104, 50.0, 3});

        SiddhiTestHelper.waitForEvents(100, 8, count, 4500);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case checks two sessions are created and processed"
            + " which belong to different session keys")
    public void testSessionWindow13() throws InterruptedException {
        log.info("SessionWindow Test13: Testing session window, providing events "
                + "which are in 2 sessions with different session keys");

        SiddhiManager siddhiManager = new SiddhiManager();

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(1 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 2) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(101, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    } else if (removeEventCount == 4) {
                        innerAssertionsPassed = false;
                        AssertJUnit.assertTrue("user1".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user1".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(103, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(104, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send(new Object[]{"user0", 102, 24.5, 2});
        Thread.sleep(10);
        inputHandler.send(new Object[]{"user1", 103, 22.4, 1});
        inputHandler.send(new Object[]{"user1", 104, 50.0, 3});

        SiddhiTestHelper.waitForEvents(100, 8, count, 4200);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case checks two sessions are created and processed after the allowedLatency")
    public void testSessionWindow14() throws InterruptedException {
        log.info("SessionWindow Test14: Testing session window, "
                + "two sessions are created and processed after the allowedLatency");

        SiddhiManager siddhiManager = new SiddhiManager();

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(2 sec, user, 1 sec) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 2) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(101, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    } else if (removeEventCount == 4) {
                        innerAssertionsPassed = false;
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(103, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(104, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send(new Object[]{"user0", 102, 24.5, 2});
        Thread.sleep(2005);

        inputHandler.send(new Object[]{"user0", 103, 22.4, 1});
        inputHandler.send(new Object[]{"user0", 104, 50.0, 3});

        SiddhiTestHelper.waitForEvents(100, 8, count, 5200);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case checks when the late event which "
            + "belongs to the current session window without an allowedLatency")
    public void testSessionWindow15() throws InterruptedException {
        log.info("SessionWindow Test15: Testing session window, "
                + "late event comes which belongs to the current session window without an allowedLatency time period");

        SiddhiManager siddhiManager = new SiddhiManager();

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(2 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 3) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[2].getData(0)));

                        AssertJUnit.assertEquals(103, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(101, removeEvents[1].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[2].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        eventTimeStamp = System.currentTimeMillis();
        inputHandler.send(eventTimeStamp, new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send((eventTimeStamp + 10), new Object[]{"user0", 102, 24.5, 2});
        //late event
        inputHandler.send((eventTimeStamp - 1000), new Object[]{"user0", 103, 24.5, 2});

        SiddhiTestHelper.waitForEvents(100, 6, count, 4100);
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(3, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case checks with the late event which "
            + "does not belong to the current session without an allowedLatency time period")
    public void testSessionWindow16() throws InterruptedException {
        log.info("SessionWindow Test16: Testing session window, "
                + "late event which does not belong to the current "
                + "session window without an allowedLatency time period");
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = config.getAppender("UnitTestAppender");

        SiddhiManager siddhiManager = new SiddhiManager();

        Logger logger = (Logger) LogManager.getLogger(SessionWindowProcessor.class);
        logger.addAppender(appender);

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(2 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 2) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));

                        AssertJUnit.assertEquals(101, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[1].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        eventTimeStamp = System.currentTimeMillis();
        inputHandler.send(eventTimeStamp, new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send((eventTimeStamp + 10), new Object[]{"user0", 102, 24.5, 2});
        //late event
        inputHandler.send((eventTimeStamp - 2500), new Object[]{"user0", 103, 24.5, 2});

        AssertJUnit.assertTrue(appender.getMessages().contains("[user0, 103, 24.5, 2]"));
        SiddhiTestHelper.waitForEvents(100, 4, count, 4100);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertEquals(2, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "This test case covers when a late event belongs "
            + "to the current session and other belongs to the previous session and another "
            + "does not belong to the previous session with allowedLatency time period")
    public void testSessionWindow17() throws InterruptedException {
        log.info("SessionWindow Test17: Testing session window, "
                + "three late events are coming, one belongs to the current session"
                + "and the another belongs to the previous session and the other "
                + "does not belong to the previous session");
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = config.getAppender("UnitTestAppender");

        SiddhiManager siddhiManager = new SiddhiManager();

        Logger logger = (Logger) LogManager.getLogger(SessionWindowProcessor.class);
        logger.addAppender(appender);

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(2 sec, user, 1 sec) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                    if (removeEventCount == 6) {
                        AssertJUnit.assertTrue("user0".equals(removeEvents[0].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[1].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[2].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[3].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[4].getData(0)));
                        AssertJUnit.assertTrue("user0".equals(removeEvents[5].getData(0)));

                        AssertJUnit.assertEquals(104, removeEvents[0].getData(1));
                        AssertJUnit.assertEquals(101, removeEvents[1].getData(1));
                        AssertJUnit.assertEquals(102, removeEvents[2].getData(1));
                        AssertJUnit.assertEquals(105, removeEvents[3].getData(1));
                        AssertJUnit.assertEquals(103, removeEvents[4].getData(1));
                        AssertJUnit.assertEquals(106, removeEvents[5].getData(1));
                        innerAssertionsPassed = true;
                    }
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        eventTimeStamp = System.currentTimeMillis();
        inputHandler.send(eventTimeStamp, new Object[]{"user0", 101, 34.4, 5});
        inputHandler.send((eventTimeStamp + 10), new Object[]{"user0", 102, 24.5, 2});
        Thread.sleep(2020);

        eventTimeStamp = System.currentTimeMillis();
        inputHandler.send(eventTimeStamp, new Object[]{"user0", 103, 24.5, 2});
        inputHandler.send((eventTimeStamp - 100), new Object[]{"user0", 104, 54.5, 12});

        inputHandler.send((eventTimeStamp - 2500), new Object[]{"user0", 105, 24.5, 6});
        inputHandler.send((eventTimeStamp - 2400), new Object[]{"user0", 106, 24.5, 2});
        //this late event later than the previous session
        inputHandler.send((eventTimeStamp - 5200), new Object[]{"user0", 107, 24.5, 2});
        AssertJUnit.assertTrue(appender.getMessages().contains("[user0, 107, 24.5, 2]"));

        SiddhiTestHelper.waitForEvents(100, 10, count, 5200);
        AssertJUnit.assertEquals(6, inEventCount);
        AssertJUnit.assertEquals(6, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check if events are persist when using session window")
    public void testSessionWindow18() throws InterruptedException {
        log.info("SessionWindow Test18: Testing persistence ");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String purchaseEventStream = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(2 sec, user) "
                + "select * "
                + "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(purchaseEventStream + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                count.addAndGet(events.length);
                for (Event event : events) {
                    innerAssertionsPassed = false;
                    AssertJUnit.assertTrue(("101".equals(event.getData(1).toString()) ||
                            "102".equals(event.getData(1).toString())) ||
                            "103".equals(event.getData(1).toString()));
                    innerAssertionsPassed = true;
                }

            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"user0", 101, 34.4, 5});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"user0", 102, 24.5, 2});

        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();

        inputHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed");
        }
        inputHandler.send(new Object[]{"user0", 103, 24.5, 2});

        SiddhiTestHelper.waitForEvents(100, 3, count, 4200);
        AssertJUnit.assertTrue(innerAssertionsPassed);

        siddhiAppRuntime.shutdown();
    }


    @Test(description = "Check whether joins are working with session window")
    public void testSessionWindow19() throws InterruptedException {
        log.info("SessionWindow Test19: testing joins");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = ""
                + "define stream purchaseEventStream (user string, item_number int, price float, quantity int); "
                + "define stream twitterStream (user string, tweet string); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(1 sec, user) "
                + "join twitterStream#window.length(2) "
                + "on purchaseEventStream.user == twitterStream.user "
                + "select purchaseEventStream.user, twitterStream.tweet, "
                + "purchaseEventStream.price, purchaseEventStream.quantity "
                + "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    count.addAndGet(events.length);
                }
                AssertJUnit.assertTrue(events[0].toString().contains("user0, Hello Tweet 1, 34.4, 5") ||
                        events[0].toString().contains("user1, Hello Tweet 2, 24.5, 12") ||
                        events[0].toString().contains("user1, Hello Tweet 2, 44.5, 12"));
                eventArrived = true;
                innerAssertionsPassed = true;
            }
        });

        InputHandler purchaseEventStreamHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
        siddhiAppRuntime.start();

        purchaseEventStreamHandler.send(new Object[]{"user0", 101, 34.4, 5});
        purchaseEventStreamHandler.send(new Object[]{"user1", 102, 24.5, 12});
        purchaseEventStreamHandler.send(new Object[]{"user1", 102, 44.5, 12});

        twitterStreamHandler.send(new Object[]{"user0", "Hello Tweet 1"});
        twitterStreamHandler.send(new Object[]{"user1", "Hello Tweet 2"});

        SiddhiTestHelper.waitForEvents(100, 10, count, 4200);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check whether aggregations are done correctly with session window")
    public void testSessionWindow20() throws InterruptedException {
        log.info("SessionWindow Test20: testing aggregations");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = ""
                + "define stream purchaseEventStream (user string, item_number int, price double, quantity int); ";

        String query = ""
                + "@info(name = 'query0') "
                + "from purchaseEventStream#window.session(1 sec, user) "
                + "select user, avg(price) as avgPrice, sum(price) as totPrice group by user "
                + "insert into outputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query0", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    count.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                    count.addAndGet(removeEvents.length);
                }
                eventArrived = true;
            }

        });

        StreamCallback callBack = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    innerAssertionsPassed = false;
                    averageValue = (Double) event.getData(1);
                    totalValue = (Double) event.getData(2);
                    innerAssertionsPassed = true;
                }
            }
        };

        siddhiAppRuntime.addCallback("outputStream", callBack);

        InputHandler purchaseEventStreamHandler = siddhiAppRuntime.getInputHandler("purchaseEventStream");
        siddhiAppRuntime.start();

        purchaseEventStreamHandler.send(new Object[]{"user0", 101, 34.4, 5});
        purchaseEventStreamHandler.send(new Object[]{"user0", 102, 24.5, 2});

        purchaseEventStreamHandler.send(new Object[]{"user1", 101, 5.0, 5});
        purchaseEventStreamHandler.send(new Object[]{"user1", 102, 6.0, 2});

        SiddhiTestHelper.waitForEvents(100, 2, count, 4200);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(Double.valueOf(29.45) == averageValue || Double.valueOf(5.5) == averageValue);
        AssertJUnit.assertTrue(Double.valueOf(58.9) == totalValue || Double.valueOf(11.0) == totalValue);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPassed);
        siddhiAppRuntime.shutdown();
    }

}

/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.siddhi.core.query.eventwindow;

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

public class UniqueExternalTimeBatchEventWindowTestCase {

    private static final Logger log = Logger.getLogger(UniqueExternalTimeBatchEventWindowTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private long sum;


    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        sum = 0;
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest1() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip, timestamp, 1 sec, 0, 2 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(3l, event.getData(2));
                        } else if (inEventCount == 2) {
                            Assert.assertEquals(2l, event.getData(2));
                        } else if (inEventCount == 3) {
                            Assert.assertEquals(3l, event.getData(2));
                        } else if (inEventCount == 4) {
                            Assert.assertEquals(4l, event.getData(2));
                        } else if (inEventCount == 5) {
                            Assert.assertEquals(2l, event.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});

        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.8"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.92"});

        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806690l, "192.10.1.10"});
        Thread.sleep(3000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 5, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest2() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 6 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804342l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804342l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814341l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814345l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824341l, "192.10.1.7"});

        Thread.sleep(1000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 2, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest3() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804342l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805340l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814341l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814741l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814641l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814545l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824341l, "192.10.1.7"});

        Thread.sleep(1000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 2, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();


    }


    @Test
    public void testUniqueExternalTimeBatchWindowTest4() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(2l, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804342l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805341l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335814341l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335814345l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335824341l, "192.10.1.7"});

        Thread.sleep(1000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 3, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();


    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest5() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test5");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 3 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(4l, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});

        Thread.sleep(5000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 1, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest6() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test6");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 3 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(4l, inEvent.getData(2));
                        } else if (inEventCount == 2) {
                            Assert.assertEquals(3l, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});

        Thread.sleep(5000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 2, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();


    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest7() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 2 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event inEvent : inEvents) {
                        inEventCount++;
                        if (inEventCount == 1) {
                            Assert.assertEquals(4l, inEvent.getData(2));
                        } else if (inEventCount == 2) {
                            Assert.assertEquals(3l, inEvent.getData(2));
                        } else if (inEventCount == 3) {
                            Assert.assertEquals(5l, inEvent.getData(2));
                        } else if (inEventCount == 4) {
                            Assert.assertEquals(2l, inEvent.getData(2));
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});
        Thread.sleep(3000);
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.7"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.8"});
        Thread.sleep(3000);
        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806690l, "192.10.1.10"});
        Thread.sleep(3000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 4, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();

    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest8() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 2 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    if (inEventCount == 1) {
                        Assert.assertEquals(4l, event.getData(2));
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(3l, event.getData(2));
                    } else if (inEventCount == 3) {
                        Assert.assertEquals(5l, event.getData(2));
                    } else if (inEventCount == 4) {
                        Assert.assertEquals(6l, event.getData(2));
                    } else if (inEventCount == 5) {
                        Assert.assertEquals(2l, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.7"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.8"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.91"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.91"});
        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806690l, "192.10.1.10"});
        Thread.sleep(3000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 5, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest9() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test9");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0, 2 sec); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    if (inEventCount == 1) {
                        Assert.assertEquals(4l, event.getData(2));
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(3l, event.getData(2));
                    } else if (inEventCount == 3) {
                        Assert.assertEquals(5l, event.getData(2));
                    } else if (inEventCount == 4) {
                        Assert.assertEquals(7l, event.getData(2));
                    } else if (inEventCount == 5) {
                        Assert.assertEquals(2l, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.7"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.8"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.91"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.92"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.92"});
        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806690l, "192.10.1.10"});
        Thread.sleep(3000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 5, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest10() throws InterruptedException {
        log.info("UniqueExternalTimeBatchWindow Test10");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream LoginEvents (timestamp long, ip string); " +
                "define window LoginEventsWindow (timestamp long, ip string) uniqueExternalTimeBatch(ip, timestamp, 1 sec, 0); ";
        String query = "" +
                "@info(name = 'query0') " +
                "from LoginEvents " +
                "insert into LoginEventsWindow ;" +
                "" +
                "@info(name = 'query1') " +
                "from LoginEventsWindow " +
                "select timestamp, ip, count() as total  " +
                "insert all events into uniqueIps ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                for (Event event : inEvents) {
                    inEventCount++;
                    if (inEventCount == 1) {
                        Assert.assertEquals(4l, event.getData(2));
                    } else if (inEventCount == 2) {
                        Assert.assertEquals(7l, event.getData(2));
                    }
                }
                eventArrived = true;
            }

        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("LoginEvents");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1366335804341l, "192.10.1.3"});
        inputHandler.send(new Object[]{1366335804599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335804607l, "192.10.1.6"});
        inputHandler.send(new Object[]{1366335805599l, "192.10.1.4"});
        inputHandler.send(new Object[]{1366335805600l, "192.10.1.5"});
        inputHandler.send(new Object[]{1366335805607l, "192.10.1.6"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.7"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.8"});
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.7"});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{1366335805606l, "192.10.1.91"});
        inputHandler.send(new Object[]{1366335805605l, "192.10.1.92"});
        inputHandler.send(new Object[]{1366335806606l, "192.10.1.9"});
        inputHandler.send(new Object[]{1366335806690l, "192.10.1.10"});
        Thread.sleep(3000);

        junit.framework.Assert.assertEquals("Event arrived", true, eventArrived);
        junit.framework.Assert.assertEquals("In Events ", 2, inEventCount);
        junit.framework.Assert.assertEquals("Remove Events ", 0, removeEventCount);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest11() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow Test11");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int, ip string); " +
                "define stream twitterStream (timestamp long, user string, tweet string, company string, ip string); " +
                "define window cseEventWindow (timestamp long, symbol string, price float, volume int, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0); " +
                "define window twitterWindow (timestamp long, user string, tweet string, company string, ip string) uniqueExternalTimeBatch(ip, timestamp, 1 sec, 0); ";

        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from twitterStream " +
                "insert into twitterWindow; " +
                "" +
                "@info(name = 'query2') " +
                "from cseEventWindow join twitterWindow " +
                "on cseEventWindow.symbol== twitterWindow.company " +
                "select cseEventWindow.symbol as symbol, twitterWindow.tweet, cseEventWindow.price " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{1366335804341l, "WSO2", 55.6f, 100, "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335804341l, "WSO2", 56.6f, 100, "1.1.1.1"});
            twitterStreamHandler.send(new Object[]{1366335804341l, "User1", "Hello World", "WSO2", "1.1.1.2"});
            twitterStreamHandler.send(new Object[]{1366335805301l, "User2", "Hello World2", "WSO2", "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335805341l, "WSO2", 75.6f, 100, "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335806541l, "WSO2", 57.6f, 100, "1.1.1.1"});
            Thread.sleep(1000);
            junit.framework.Assert.assertEquals(2, inEventCount);
            junit.framework.Assert.assertEquals(0, removeEventCount);
            junit.framework.Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void testUniqueExternalTimeBatchWindowTest12() throws InterruptedException {
        log.info("uniqueExternalTimeBatchWindow Test12");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream cseEventStream (timestamp long, symbol string, price float, volume int, ip string); " +
                "define stream twitterStream (timestamp long, user string, tweet string, company string, ip string); " +
                "define window cseEventWindow (timestamp long, symbol string, price float, volume int, ip string) uniqueExternalTimeBatch(ip,timestamp, 1 sec, 0); " +
                "define window twitterWindow (timestamp long, user string, tweet string, company string, ip string) uniqueExternalTimeBatch(ip, timestamp, 1 sec, 0); ";

        String query = "" +
                "@info(name = 'query0') " +
                "from cseEventStream " +
                "insert into cseEventWindow; " +
                "" +
                "@info(name = 'query1') " +
                "from twitterStream " +
                "insert into twitterWindow; " +
                "" +
                "@info(name = 'query2') " +
                "from cseEventWindow join twitterWindow " +
                "on cseEventWindow.symbol== twitterWindow.company " +
                "select cseEventWindow.symbol as symbol, twitterWindow.tweet, cseEventWindow.price " +
                "insert all events into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        try {
            executionPlanRuntime.addCallback("query2", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = executionPlanRuntime.getInputHandler("twitterStream");
            executionPlanRuntime.start();
            cseEventStreamHandler.send(new Object[]{1366335804341l, "WSO2", 55.6f, 100, "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335804341l, "WSO2", 56.6f, 100, "1.1.1.1"});
            twitterStreamHandler.send(new Object[]{1366335804341l, "User1", "Hello World", "WSO2", "1.1.1.2"});
            twitterStreamHandler.send(new Object[]{1366335805301l, "User2", "Hello World2", "WSO2", "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335805341l, "WSO2", 75.6f, 100, "1.1.1.1"});
            cseEventStreamHandler.send(new Object[]{1366335806541l, "WSO2", 57.6f, 100, "1.1.1.1"});
            Thread.sleep(1000);
            junit.framework.Assert.assertEquals(2, inEventCount);
            junit.framework.Assert.assertEquals(1, removeEventCount);
            junit.framework.Assert.assertTrue(eventArrived);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

}

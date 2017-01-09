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
package org.wso2.siddhi.core.debugger;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class TestDebugger {
    private static final Logger log = Logger.getLogger(TestDebugger.class);
    private AtomicInteger inEventCount = new AtomicInteger(0);
    private AtomicInteger debugEventCount = new AtomicInteger(0);
    private static volatile int count;

    @Before
    public void init() {
        inEventCount.set(0);
        debugEventCount.set(0);
    }

    private int getCount(ComplexEvent event) {
        int count = 0;
        while (event != null) {
            count++;
            event = event.getNext();
        }

        return count;
    }

    @Test
    public void testDebugger1() throws InterruptedException {
        log.info("Siddi Debugger Test 1: Test next traversal in a simple query");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
                "volume int);";
        final String query = "@info(name = 'query 1')" +
                "from cseEventStream " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query 1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query 1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query 1OUT", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at OUT", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 3) {
                    Assert.assertEquals("Incorrect break point", "query 1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                } else if (count == 4) {
                    Assert.assertEquals("Incorrect break point", "query 1OUT", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at OUT", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                }
                debugger.next();
            }
        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 2, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 4, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger2() throws InterruptedException {
        log.info("Siddi Debugger Test 2: Test next traversal in a query with length batch window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
                "volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.lengthBatch(3) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                } else if (count == 3) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 60f, 50},
                            event.getOutputData());
                } else if (count == 4) {
                    Assert.assertEquals("Incorrect break point", "query1OUT", queryName + queryTerminal);
                    Assert.assertEquals("Incorrect number of events received", 3, getCount(event));
                }
                debugger.next();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 6, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger3() throws InterruptedException {
        log.info("Siddi Debugger Test 3: Test next traversal in a query with time batch window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.timeBatch(3 sec) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + "\t" + System.currentTimeMillis());
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                } else if (count == 3) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 60f, 50},
                            event.getOutputData());
                }

                // next call will not reach OUT since there is a window
                debugger.next();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(3500);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 3, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger4() throws InterruptedException {
        log.info("Siddi Debugger Test 4: Test next traversal in a query with time batch window where next call delays" +
                " 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
                Assert.assertEquals("Cannot emit all three in one time", 1, events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));

                if (count != 1 && queryTerminal == SiddhiDebugger.QueryTerminal.IN) {
                    try {
                        Thread.sleep(1100);
                    } catch (InterruptedException e) {
                    }
                }
                // next call will not reach OUT since there is a window
                debugger.next();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(1500);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 3, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger5() throws InterruptedException {
        log.info("Siddi Debugger Test 5: Test play in a simple query");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
                "volume int);";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at OUT", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                }

                debugger.play();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 2, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 2, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }


    @Test
    public void testDebugger6() throws InterruptedException {
        log.info("Siddi Debugger Test 6: Test play traversal in a query with length batch window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
                "volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.lengthBatch(3) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                } else if (count == 3) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 60f, 50},
                            event.getOutputData());
                }
                debugger.play();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 3, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger7() throws InterruptedException {
        log.info("Siddi Debugger Test 7: Test play traversal in a query with time batch window");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.timeBatch(3 sec) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + "\t" + System.currentTimeMillis());
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 1) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 50f, 60},
                            event.getOutputData());
                } else if (count == 2) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 70f, 40},
                            event.getOutputData());
                } else if (count == 3) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                    Assert.assertArrayEquals("Incorrect debug event received at IN", new Object[]{"WSO2", 60f, 50},
                            event.getOutputData());
                }

                debugger.play();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(3500);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 3, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger8() throws InterruptedException {
        log.info("Siddi Debugger Test 8: Test play traversal in a query with time batch window where play call delays" +
                " 1 sec");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select symbol, price, volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                Assert.assertEquals("Only one event can be emitted from the window", 1, getCount(event));

                if (count != 1 && "query1IN".equals(queryName)) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
                debugger.play();
            }


        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});
        inputHandler.send(new Object[]{"WSO2", 60f, 50});

        Thread.sleep(1500);

        Assert.assertEquals("Invalid number of output events", 3, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 3, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger9() throws InterruptedException {
        log.info("Siddi Debugger Test 9: Test state traversal in a simple query");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
                "volume int);";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream#window.length(3) " +
                "select symbol, price, sum(volume) as volume " +
                "insert into OutputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if (count == 2) {
                    QueryState queryState = debugger.getQueryState(queryName);
                    System.out.println(queryState);
                    StreamEvent streamEvent = (StreamEvent) ((Map<String, Object>) queryState.getKnownFields().values
                            ().toArray()[0]).get("ExpiredEventChunk");
                    Assert.assertArrayEquals(streamEvent.getOutputData(), new Object[]{"WSO2", 50.0f, null});
                }
                debugger.next();
            }
        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});

//        System.out.println(siddhiDebugger.getQueryState("query1"));
        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 2, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 4, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger10() throws InterruptedException {
        log.info("Siddi Debugger Test 10: Test next traversal in a query with two consequent streams");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream stockEventStream (symbol string, price float, volume int); ";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream " +
                "select symbol, price, volume " +
                "insert into stockEventStream; " +
                "@info(name = 'query2')" +
                "from stockEventStream " +
                "select * " +
                "insert into OutputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));
                if ((count - 1) / 4 == 0) {
                    // First four events
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"WSO2", 50f, 60}, event
                            .getOutputData());
                } else {
                    // Next four events
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"WSO2", 70f, 40}, event
                            .getOutputData());
                }
                if (count == 1 || count == 5) {
                    Assert.assertEquals("Incorrect break point", "query1IN", queryName + queryTerminal);
                } else if (count == 2 || count == 6) {
                    Assert.assertEquals("Incorrect break point", "query1OUT", queryName + queryTerminal);
                } else if (count == 3 || count == 7) {
                    Assert.assertEquals("Incorrect break point", "query2IN", queryName + queryTerminal);
                } else {
                    Assert.assertEquals("Incorrect break point", "query2OUT", queryName + queryTerminal);
                }

                debugger.next();
            }
        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});
        inputHandler.send(new Object[]{"WSO2", 70f, 40});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 2, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 8, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger11() throws InterruptedException {
        log.info("Siddi Debugger Test 11: Modify events during debug mode");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream stockEventStream (symbol string, price float, volume int); ";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream " +
                "select symbol, price, volume " +
                "insert into stockEventStream; " +
                "@info(name = 'query2')" +
                "from stockEventStream " +
                "select * " +
                "insert into OutputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);

                int count = debugEventCount.addAndGet(getCount(event));

                if ((count - 1) / 2 == 0) {
                    // WSO2 in stream 1
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"WSO2", 50f, 60}, event
                            .getOutputData());
                } else {
                    // IBM in stream 2
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"IBM", 50f, 60}, event
                            .getOutputData());
                }

                if (count == 2) {
                    // Modify the event at the end of the first stream
                    event.getOutputData()[0] = "IBM";
                }

                debugger.next();
            }
        });

        inputHandler.send(new Object[]{"WSO2", 50f, 60});

        Thread.sleep(100);

        Assert.assertEquals("Invalid number of output events", 1, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 4, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testDebugger12() throws InterruptedException {
        log.info("Siddi Debugger Test 12: Test debugging two queries with concurrent input");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') " +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream stockEventStream (symbol string, price float, volume int); ";
        final String query = "@info(name = 'query1')" +
                "from cseEventStream " +
                "select * " +
                "insert into OutputStream1; " +
                "@info(name = 'query2')" +
                "from stockEventStream " +
                "select * " +
                "insert into OutputStream2;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("OutputStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        executionPlanRuntime.addCallback("OutputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                inEventCount.addAndGet(events.length);
            }
        });
        final InputHandler cseEventStreamInputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        final InputHandler stockEventStreamInputHandler = executionPlanRuntime.getInputHandler("stockEventStream");

        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        siddhiDebugger.acquireBreakPoint("query1", SiddhiDebugger.QueryTerminal.IN);
        siddhiDebugger.acquireBreakPoint("query2", SiddhiDebugger.QueryTerminal.IN);

        siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
            private AtomicBoolean queryOneResumed = new AtomicBoolean(false);

            @Override
            public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                                   SiddhiDebugger debugger) {
                System.out.println("Query: " + queryName + ":" + queryTerminal);
                System.out.println(event);
                debugEventCount.addAndGet(getCount(event));
                if ("query1IN".equals(queryName)) {
                    try {
                        Thread.sleep(1000);     // Wait for 1 sec
                        this.queryOneResumed.set(true);
                    } catch (InterruptedException e) {
                    }
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"WSO2", 50f, 60}, event
                            .getOutputData());
                } else if ("query2IN".equals(queryName)) {
                    // If query2IN is reached, query1IN must left that break point
                    Assert.assertTrue("Query 2 thread enterted the checkpoint before query 1 is debugged",
                            queryOneResumed.get());
                    Assert.assertArrayEquals("Incorrect debug event received", new Object[]{"IBM", 45f, 80}, event
                            .getOutputData());
                }
                debugger.next();
            }
        });

        new Thread() {
            @Override
            public void run() {
                try {
                    cseEventStreamInputHandler.send(new Object[]{"WSO2", 50f, 60});
                } catch (InterruptedException e) {

                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10);
                    stockEventStreamInputHandler.send(new Object[]{"IBM", 45f, 80});
                } catch (InterruptedException e) {

                }
            }
        }.start();

        Thread.sleep(2000);

        Assert.assertEquals("Invalid number of output events", 2, inEventCount.get());
        Assert.assertEquals("Invalid number of debug events", 4, debugEventCount.get());

        executionPlanRuntime.shutdown();
    }
}

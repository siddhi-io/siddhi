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

package io.siddhi.core.query.pattern;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class CountPatternTestCase {

    private static final Logger log = LogManager.getLogger(CountPatternTestCase.class);
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
    public void testQuery1() throws InterruptedException {
        log.info("testPatternCount1 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 47.6f, 47.8f, null, 45.7f},
                            inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 13.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.8f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery2() throws InterruptedException {
        log.info("testPatternCount2 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 47.6f, null, null, 45.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 13.7f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.8f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery3() throws InterruptedException {
        log.info("testPatternCount3 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 47.8f, null, null, 55.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.8f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery4() throws InterruptedException {
        log.info("testPatternCount4 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                AssertJUnit.fail();
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 0, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", false, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery5() throws InterruptedException {
        log.info("testPatternCount5 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, " +
                "   e1[3].price as price1_3, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 47.6f, 23.7f, 24.7f, 45.7f},
                            inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 23.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 24.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 25.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 27.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.8f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery6() throws InterruptedException {
        log.info("testPatternCount6 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>e1[1].price] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 47.6f, 55.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery7() throws InterruptedException {
        log.info("testPatternCount7 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <0:5> -> e2=Stream2[price>20] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{null, null, 45.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery8() throws InterruptedException {
        log.info("testPatternCount8 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <0:5> -> e2=Stream2[price>e1[0].price] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e2.price as price2 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, null, 45.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 7.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery9() throws InterruptedException {
        log.info("testPatternCount9 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream EventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1 = EventStream [price >= 50 and volume > 100] -> e2 = EventStream [price <= 40] <0:5> " +
                "   -> e3 = EventStream [volume <= 70] " +
                "select e1.symbol as symbol1, e2[0].symbol as symbol2, e3.symbol as symbol3 " +
                "insert into StockQuote;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", "GOOG", "WSO2"}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(1, inEventCount);
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");

        siddhiAppRuntime.start();

        eventStream.send(new Object[]{"IBM", 75.6f, 105});
        Thread.sleep(100);
        eventStream.send(new Object[]{"GOOG", 21f, 81});
        eventStream.send(new Object[]{"WSO2", 176.6f, 65});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery10() throws InterruptedException {
        log.info("testPatternCount10 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream EventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1 = EventStream [price >= 50 and volume > 100] -> e2 = EventStream [price <= 40] <:5> " +
                "   -> e3 = EventStream [volume <= 70] " +
                "select e1.symbol as symbol1, e2[0].symbol as symbol2, e3.symbol as symbol3 " +
                "insert into StockQuote;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", null, "GOOG"}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(1, inEventCount);
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");

        siddhiAppRuntime.start();

        eventStream.send(new Object[]{"IBM", 75.6f, 105});
        Thread.sleep(100);
        eventStream.send(new Object[]{"GOOG", 21f, 61});
        eventStream.send(new Object[]{"WSO2", 21f, 61});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery11() throws InterruptedException {
        log.info("testPatternCount11 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream EventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1 = EventStream [price >= 50 and volume > 100] -> e2 = EventStream [price <= 40] <:5> " +
                "   -> e3 = EventStream [volume <= 70] " +
                "select e1.symbol as symbol1, e2[last].symbol as symbol2, e3.symbol as symbol3 " +
                "insert into StockQuote;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", null, "GOOG"}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(1, inEventCount);
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");

        siddhiAppRuntime.start();

        eventStream.send(new Object[]{"IBM", 75.6f, 105});
        Thread.sleep(100);
        eventStream.send(new Object[]{"GOOG", 21f, 61});
        eventStream.send(new Object[]{"WSO2", 21f, 61});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery12() throws InterruptedException {
        log.info("testPatternCount12 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream EventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1 = EventStream [price >= 50 and volume > 100] -> e2 = EventStream [price <= 40] <:5> " +
                "   -> e3 = EventStream [volume <= 70] " +
                "select e1.symbol as symbol1, e2[last].symbol as symbol2, e3.symbol as symbol3 " +
                "insert into StockQuote;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", "FB", "WSO2"}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(1, inEventCount);
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");

        siddhiAppRuntime.start();

        eventStream.send(new Object[]{"IBM", 75.6f, 105});
        Thread.sleep(100);
        eventStream.send(new Object[]{"GOOG", 21f, 91});
        eventStream.send(new Object[]{"FB", 21f, 81});
        eventStream.send(new Object[]{"WSO2", 21f, 61});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery13() throws InterruptedException {
        log.info("testPatternCount13 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream EventStream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1 = EventStream -> " +
                "     e2 = EventStream [e1.symbol==e2.symbol]<4:6> " +
                "select e1.volume as volume1, e2[0].volume as volume2, e2[1].volume as volume3, e2[2].volume as " +
                "volume4, e2[3].volume as volume5, e2[4].volume as volume6, e2[5].volume as volume7 " +
                "insert into StockQuote;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{100, 200, 300, 400, 500, null, null}, event
                                        .getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{200, 300, 400, 500, 600, null, null}, event
                                        .getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{300, 400, 500, 600, 700, null, null}, event
                                        .getData());
                                break;
                            case 4:
                                AssertJUnit.assertArrayEquals(new Object[]{400, 500, 600, 700, 800, null, null}, event
                                        .getData());
                                break;
                            case 5:
                                AssertJUnit.assertArrayEquals(new Object[]{500, 600, 700, 800, 900, null, null}, event
                                        .getData());
                                break;
                            default:
                                AssertJUnit.assertSame(5, inEventCount);
                        }
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");

        siddhiAppRuntime.start();

        eventStream.send(new Object[]{"IBM", 75.6f, 100});
        eventStream.send(new Object[]{"IBM", 75.6f, 200});
        eventStream.send(new Object[]{"IBM", 75.6f, 300});
        eventStream.send(new Object[]{"GOOG", 21f, 91});
        eventStream.send(new Object[]{"IBM", 75.6f, 400});
        eventStream.send(new Object[]{"IBM", 75.6f, 500});

        eventStream.send(new Object[]{"GOOG", 21f, 91});

        eventStream.send(new Object[]{"IBM", 75.6f, 600});
        eventStream.send(new Object[]{"IBM", 75.6f, 700});
        eventStream.send(new Object[]{"IBM", 75.6f, 800});
        eventStream.send(new Object[]{"GOOG", 21f, 91});
        eventStream.send(new Object[]{"IBM", 75.6f, 900});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 5, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery14() throws InterruptedException {
        log.info("testPatternCount14 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <0:5> -> e2=Stream2[price>e1[0].price] " +
                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, e2.price as price2" +
                " " +
                "having instanceOfFloat(e1[1].price) and not instanceOfFloat(e1[2].price) and instanceOfFloat" +
                "(price1_1) and not instanceOfFloat(price1_2) " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{25.6f, 23.6f, null, 45.7f}, inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 23.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 7.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery15() throws InterruptedException {
        log.info("testPatternCount15 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> e2=Stream1[price>20]<2> -> not Stream1[price>20] and e3=Stream2 " +
                "select e1.price as price1_0, e2[0].price as price2_0, e2[1].price as price2_1, " +
                "e2[2].price as price2_2, e3.price as price3_0 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{23.6f, 27.6f, 28.6f, null, 45.7f},
                            inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 23.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 23.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 27.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 28.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7f, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery16() throws InterruptedException {
        log.info("testQuery16");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "@app:playback\n " +
                "define stream Stream1 (id string, symbol string, price float, volume int); ";
        String query = "" +
                " @info(name = 'query1') " +
                " from every e1=Stream1[symbol=='WSO2'] " +
                "  -> e2=Stream1[symbol=='WSO2']<2:> -> e3=Stream1[symbol=='GOOG'] " +
                " within 10 milliseconds " +
                " select e1.price as price1, e2.price as price2, e3.price as price3 " +
                " insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                eventArrived = true;
                inEventCount += inEvents.length;
            }

        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        siddhiAppRuntime.start();

        long startTime = System.currentTimeMillis();
        long now = 1;
        for (int i = 1; i < 401; i++) {
            stream1.send(++now, new Object[]{
                    ++now, "WSO2", 25.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "WSO2", 23.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "WSO2", 23.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "WSO2", 23.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "WSO2", 23.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "GOOG", 27.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "GOOG", 28.6f, 100
            });
            stream1.send(++now, new Object[]{
                    ++now, "GOOG", 28.6f, 100
            });
            now += 100;
        }

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("Event count", 400 * 3, inEventCount);
        AssertJUnit.assertTrue("Event processing time", timeElapsed < 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery17() throws InterruptedException {
        log.info("testQuery17");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream InputStream (name string); \n" +
                "@info(name = 'query1') " +
                "from every e1=InputStream[(e1.name == 'A')]<2> " +
                "   -> e2=InputStream[(e2.name == 'B')] " +
                "   within 3 seconds " +
                "select 'rule1' as ruleId, count() as numOfEvents " +
                "insert into OutputStream";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                eventArrived = true;
                inEventCount += inEvents.length;
            }

        });

        InputHandler inputStream = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});


        inputStream.send(new Object[]{"A"});
        Thread.sleep(4000);
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});


        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("Event count", 3, inEventCount);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery18() throws InterruptedException {
        log.info("testQuery18");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream InputStream (name string); \n" +
                "@info(name = 'query1') " +
                "from every e1=InputStream[(e1.name == 'A')]<2> " +
                "   -> e2=InputStream[(e2.name == 'B')]<2> " +
                "   within 3 seconds " +
                "select 'rule1' as ruleId, count() as numOfEvents " +
                "insert into OutputStream";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                eventArrived = true;
                inEventCount += inEvents.length;
            }

        });

        InputHandler inputStream = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        Thread.sleep(4000);
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("Event count", 3, inEventCount);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery19() throws InterruptedException {
        log.info("testQuery19");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream InputStream (name string); \n" +
                "@info(name = 'query1') " +
                "from every e1=InputStream[(e1.name == 'A')]<2> " +
                "   -> e2=InputStream[(e2.name == 'B')]<2:> " +
                "   within 3 seconds " +
                "select 'rule1' as ruleId, count() as numOfEvents " +
                "insert into OutputStream";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                eventArrived = true;
                inEventCount += inEvents.length;
            }

        });

        InputHandler inputStream = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});


        inputStream.send(new Object[]{"A"});
        Thread.sleep(4000);
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("Event count", 4, inEventCount);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery20() throws InterruptedException {
        log.info("testQuery20");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream InputStream (name string); \n" +
                "@info(name = 'query1') " +
                "from e1=InputStream[(e1.name == 'A')]<2> " +
                "   -> every e2=InputStream[(e2.name == 'B')]<2> " +
                "   within 3 seconds " +
                "select 'rule1' as ruleId, count() as numOfEvents " +
                "insert into OutputStream";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                eventArrived = true;
                inEventCount += inEvents.length;
            }

        });

        InputHandler inputStream = siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start();

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        Thread.sleep(4000);
        inputStream.send(new Object[]{"B"});

        //AA are not consumed after within time period
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"A"});
        inputStream.send(new Object[]{"B"});
        inputStream.send(new Object[]{"B"});

        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        AssertJUnit.assertEquals("Event count", 2, inEventCount);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery21() throws InterruptedException {
        log.info("testQuery21 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price double, volume int); " +
                "define stream Stream2 (symbol string, price double, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] <2:5> -> e2=Stream2[price>20] " +
                "select e1.price as prices, e1[0].price as price0 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        StreamDefinition streamDefinition = siddhiAppRuntime.getStreamDefinitionMap().get("OutputStream");
        Assert.assertEquals(Attribute.Type.OBJECT, streamDefinition.getAttributeType("prices"));
        Assert.assertEquals(Attribute.Type.DOUBLE, streamDefinition.getAttributeType("price0"));

        List results = new ArrayList();
        results.add(25.6);
        results.add(47.6);
        results.add(47.8);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    AssertJUnit.assertArrayEquals(new Object[]{results, 25.6},
                            inEvents[0].getData());
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 25.6, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.6, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 13.7, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 47.8, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 45.7, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7, 100});
        Thread.sleep(100);

        AssertJUnit.assertEquals("Number of success events", 1, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery22() throws InterruptedException {
        log.info("testQuery22 - OUT 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@app:playback\n" +
                "define stream LoginFailure (id string, user string, type string);\n" +
                "define stream LoginSuccess (id string, user string, type string);\n" +
                "\n" +
                "partition with (user of LoginFailure, user of LoginSuccess)\n" +
                "begin\n" +
                "\n" +
                "  from every (e1=LoginFailure<3:> -> e2=LoginSuccess) \n" +
                "  select e1[0].id as id, e2.user as user\n" +
                "  insert into BreakIn\n" +
                "\n" +
                "end;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("BreakIn", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    for (Event event : events) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(0), "id_1");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(0), "id_8");
                                Assert.assertEquals(event.getData(1), "werner");
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(0), "id_17");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }


        });

        InputHandler failureInput = siddhiAppRuntime.getInputHandler("LoginFailure");
        InputHandler successInput = siddhiAppRuntime.getInputHandler("LoginSuccess");

        siddhiAppRuntime.start();
        long now = System.currentTimeMillis();
        failureInput.send(++now, new Object[]{"id_1", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_2", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_3", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_4", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_5", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_6", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_7", "hans", "success"});

        failureInput.send(++now, new Object[]{"id_8", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_9", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_10", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_11", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_12", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_13", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_14", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_15", "werner", "failure"});
        successInput.send(++now, new Object[]{"id_16", "werner", "success"});
        now += 3 * 1000; //add 3 sec delay

        failureInput.send(++now, new Object[]{"id_17", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_18", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_19", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_20", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_21", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_22", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_23", "hans", "success"});

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(inEventCount, 3);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery23() throws InterruptedException {
        log.info("testQuery23 - OUT 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@app:playback\n" +
                "define stream LoginFailure (id string, user string, type string);\n" +
                "define stream LoginSuccess (id string, user string, type string);\n" +
                "\n" +
                "partition with (user of LoginFailure, user of LoginSuccess)\n" +
                "begin\n" +
                "\n" +
                "  from every (e1=LoginFailure<3:> -> e2=LoginSuccess) \n" +
                "  select e1[0].id as id, e2.user as user\n" +
                "  insert into BreakIn\n" +
                "\n" +
                "end;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("BreakIn", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    for (Event event : events) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(0), "id_1");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(0), "id_11");
                                Assert.assertEquals(event.getData(1), "werner");
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(0), "id_19");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }


        });

        InputHandler failureInput = siddhiAppRuntime.getInputHandler("LoginFailure");
        InputHandler successInput = siddhiAppRuntime.getInputHandler("LoginSuccess");

        siddhiAppRuntime.start();
        long now = System.currentTimeMillis();
        failureInput.send(++now, new Object[]{"id_1", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_2", "hans", "failure"});

        failureInput.send(++now, new Object[]{"id_11", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_12", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_13", "werner", "failure"});

        failureInput.send(++now, new Object[]{"id_3", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_4", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_5", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_6", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_7", "hans", "success"});

        failureInput.send(++now, new Object[]{"id_8", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_9", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_10", "werner", "failure"});

        failureInput.send(++now, new Object[]{"id_19", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_20", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_21", "hans", "failure"});

        failureInput.send(++now, new Object[]{"id_14", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_15", "werner", "failure"});
        successInput.send(++now, new Object[]{"id_16", "werner", "success"});
        now += 3 * 1000; //add 3 sec delay

        failureInput.send(++now, new Object[]{"id_17", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_18", "hans", "failure"});

        failureInput.send(++now, new Object[]{"id_22", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_23", "hans", "success"});

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(inEventCount, 3);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery24() throws InterruptedException {
        log.info("testQuery24 - OUT 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@app:playback\n" +
                "define stream LoginFailure (id string, user string, type string);\n" +
                "define stream LoginSuccess (id string, user string, type string);\n" +
                "\n" +
                "  from every (e1=LoginFailure<3:> -> e2=LoginSuccess) \n" +
                "  select e1[0].id as id, e2.user as user\n" +
                "  insert into BreakIn\n" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("BreakIn", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    for (Event event : events) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(0), "id_1");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(0), "id_8");
                                Assert.assertEquals(event.getData(1), "werner");
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(0), "id_17");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }


        });

        InputHandler failureInput = siddhiAppRuntime.getInputHandler("LoginFailure");
        InputHandler successInput = siddhiAppRuntime.getInputHandler("LoginSuccess");

        siddhiAppRuntime.start();
        long now = System.currentTimeMillis();
        failureInput.send(++now, new Object[]{"id_1", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_2", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_3", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_4", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_5", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_6", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_7", "hans", "success"});
        successInput.send(++now, new Object[]{"id_7_1", "hans", "success"});

        failureInput.send(++now, new Object[]{"id_8", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_9", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_10", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_11", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_12", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_13", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_14", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_15", "werner", "failure"});
        successInput.send(++now, new Object[]{"id_16", "werner", "success"});
        now += 3 * 1000; //add 3 sec delay

        failureInput.send(++now, new Object[]{"id_17", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_18", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_18_1", "hans", "success"});
        failureInput.send(++now, new Object[]{"id_19", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_20", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_21", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_22", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_23", "hans", "success"});

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(inEventCount, 3);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery25() throws InterruptedException {
        log.info("testQuery25 - OUT 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@app:playback\n" +
                "define stream LoginFailure (id string, user string, type string);\n" +
                "define stream LoginSuccess (id string, user string, type string);\n" +
                "\n" +
                "partition with (user of LoginFailure, user of LoginSuccess)\n" +
                "begin\n" +
                "\n" +
                "  from every (e1=LoginFailure<3:> -> e2=LoginSuccess) within 2 sec  \n" +
                "  select e1[0].id as id, e2.user as user\n" +
                "  insert into BreakIn\n" +
                "\n" +
                "end;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("BreakIn", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    for (Event event : events) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(0), "id_8");
                                Assert.assertEquals(event.getData(1), "werner");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(0), "id_17");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }


        });

        InputHandler failureInput = siddhiAppRuntime.getInputHandler("LoginFailure");
        InputHandler successInput = siddhiAppRuntime.getInputHandler("LoginSuccess");

        siddhiAppRuntime.start();
        long now = System.currentTimeMillis();
        failureInput.send(++now, new Object[]{"id_1", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_2", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_3", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_4", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_5", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_6", "hans", "failure"});
//        successInput.send(++now, new Object[]{"id_7", "hans", "success"});

        failureInput.send(++now, new Object[]{"id_8", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_9", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_10", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_11", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_12", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_13", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_14", "werner", "failure"});
        failureInput.send(++now, new Object[]{"id_15", "werner", "failure"});
        successInput.send(++now, new Object[]{"id_16", "werner", "success"});
        now += 3 * 1000; //add 3 sec delay

        failureInput.send(++now, new Object[]{"id_17", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_18", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_19", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_20", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_21", "hans", "failure"});
        failureInput.send(++now, new Object[]{"id_22", "hans", "failure"});
        successInput.send(++now, new Object[]{"id_23", "hans", "success"});

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(inEventCount, 2);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery26() throws InterruptedException {
        log.info("testQuery26 - OUT 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@app:playback " +
                "define stream AuthenticationStream (id string, user string, type string);\n" +
                "\n" +
                "@purge(enable='true', interval='1 sec', idle.period='2 sec')\n" +
                "partition with (user of AuthenticationStream)\n" +
                "begin\n" +
                "    from every (e1=AuthenticationStream[type == 'failure' ]<1:> -> \n" +
                "                        e2=AuthenticationStream[type == 'success' ]) within  1 sec \n" +
                "    select e1[0].id as id, e1[0].user as user, e1[3].id as id4\n" +
                "    having not(id4 is null)\n" +
                "    insert into BreakIn\n" +
                "end;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("BreakIn", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                if (events != null) {
                    for (Event event : events) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(0), "id_8");
                                Assert.assertEquals(event.getData(1), "werner");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(0), "id_17");
                                Assert.assertEquals(event.getData(1), "hans");
                                break;
                        }
                    }
                    eventArrived = true;
                }
            }


        });

        InputHandler authenticationStream = siddhiAppRuntime.getInputHandler("AuthenticationStream");

        siddhiAppRuntime.start();
        long now = System.currentTimeMillis();
        authenticationStream.send(++now, new Object[]{"id_1", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_2", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_3", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_4", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_5", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_6", "hans", "failure"});

        authenticationStream.send(++now, new Object[]{"id_8", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_9", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_10", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_11", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_12", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_13", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_14", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_15", "werner", "failure"});
        authenticationStream.send(++now, new Object[]{"id_16", "werner", "success"});
        now += 3 * 1000; //add 3 sec delay

        authenticationStream.send(++now, new Object[]{"id_7", "hans", "success"});

        authenticationStream.send(++now, new Object[]{"id_17", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_18", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_19", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_20", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_21", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_22", "hans", "failure"});
        authenticationStream.send(++now, new Object[]{"id_23", "hans", "success"});

        authenticationStream.send(++now, new Object[]{"id_21", "ben", "failure"});
        authenticationStream.send(++now, new Object[]{"id_22", "ben", "failure"});
        authenticationStream.send(++now, new Object[]{"id_23", "ben", "success"});

        Assert.assertTrue(eventArrived);
        Assert.assertEquals(inEventCount, 2);
        siddhiAppRuntime.shutdown();
    }

}

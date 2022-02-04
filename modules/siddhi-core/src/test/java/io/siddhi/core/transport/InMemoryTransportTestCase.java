/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.transport;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.config.InMemoryConfigManager;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryTransportTestCase {
    private static final Logger log = LogManager.getLogger(InMemoryTransportTestCase.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }

    @Test
    public void inMemorySinkAndEventMappingWithSiddhiQLDynamicParams() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemorySinkAndEventMappingWithSiddhiQLDynamicParams"})
    public void inMemorySinkAndEventMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='IBM', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 0, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 3, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemorySinkAndEventMappingWithSiddhiQL"})
    public void inMemorySourceAndEventMappingWithSiddhiQL() throws InterruptedException,
            SubscriberUnAvailableException {
        log.info("Test inMemorySource And EventMapping With SiddhiQL");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='WSO2', @map(type='passThrough')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (wso2Count.incrementAndGet()) {
                        case 1:
                            org.testng.AssertJUnit.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            org.testng.AssertJUnit.assertEquals(57.6f, event.getData(1));
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"inMemorySourceAndEventMappingWithSiddhiQL"})
    public void inMemorySourceSinkAndEventMappingWithSiddhiQL() throws InterruptedException,
            SubscriberUnAvailableException {
        log.info("Test inMemory Source Sink And EventMapping With SiddhiQL");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='Foo', @map(type='passThrough')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        InMemoryBroker.publish("IBM", new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemorySourceSinkAndEventMappingWithSiddhiQL"})
    public void inMemorySourceSinkAndEventMappingWithSiddhiQL2() throws InterruptedException,
            SubscriberUnAvailableException {
        log.info("Test inMemory Source Sink And EventMapping With SiddhiQL2");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='Foo', @map(type='passThrough')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into FooBarStream; " +
                "" +
                "from FooBarStream " +
                "select * " +
                "insert into BarStream; " +
                "";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        InMemoryBroker.publish("IBM", new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class,
            dependsOnMethods = {"inMemorySourceSinkAndEventMappingWithSiddhiQL2"})
    public void inMemoryTestCase3() throws InterruptedException {
        log.info("Test inMemory 3");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='Foo', @map(type='passThrough', @attributes(symbol='symbol'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"inMemoryTestCase3"})
    public void inMemoryTestCase4() throws InterruptedException {
        log.info("Test inMemory 4");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='Foo', @map(type='passThrough', @attributes('symbol','price'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

    }

    @Test(dependsOnMethods = {"inMemoryTestCase4"})
    public void inMemoryTestCase5() throws InterruptedException {
        log.info("Test inMemory 5");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='Foo', @map(type='passThrough', @attributes(symbol='symbol'," +
                "volume='volume',price='price'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

    }

    @Test(dependsOnMethods = {"inMemoryTestCase5"})
    public void inMemoryTestCase6() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 6");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='Foo', prop1='hi', prop2='test', " +
                "   @map(type='passThrough', @attributes(symbol='trp:symbol'," +
                "        volume='volume',price='trp:price'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
                for (Event event : events) {
                    AssertJUnit.assertArrayEquals(event.getData(), new Object[]{"hi", "test", 100L});
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"IBM", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase6"})
    public void inMemoryTestCase7() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 7");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='Foo', prop1='hi', prop2='test', fail='true', " +
                "   @map(type='passThrough', @attributes(symbol='trp:symbol'," +
                "        volume='volume',price='trp:price'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
                for (Event event : events) {
                    AssertJUnit.assertArrayEquals(event.getData(), new Object[]{"hi", "test", 100L});
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 0, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase7"})
    public void inMemoryWithFailingSink() throws InterruptedException {
        log.info("Test failing inMemorySink");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testFailingInMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        TestFailingInMemorySink.fail = true;
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        TestFailingInMemorySink.fail = false;
        Thread.sleep(5500);
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 1, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 2, ibmCount.get());
        AssertJUnit.assertEquals("Number of errors", 3, TestFailingInMemorySink.numberOfErrorOccurred);
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemoryWithFailingSink"})
    public void inMemoryWithFailingSink1() throws InterruptedException {
        log.info("Test failing inMemorySink1");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testFailingInMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        TestFailingInMemorySink.publishAlwaysFail = true;

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.7f, 100L});
        Thread.sleep(5500);
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 0, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 0, ibmCount.get());
        AssertJUnit.assertEquals("Number of errors", 10, TestFailingInMemorySink.numberOfErrorOccurred);
        siddhiAppRuntime.shutdown();

        TestFailingInMemorySink.publishAlwaysFail = false;

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }


    @Test(dependsOnMethods = {"inMemoryWithFailingSink1"})
    public void inMemoryWithFailingSource() throws InterruptedException {
        log.info("Test failing inMemorySource");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testFailingInMemory', topic='WSO2', @map(type='passThrough')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    wso2Count.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        try {
            InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        } catch (SubscriberUnAvailableException e) {
            AssertJUnit.fail();
        }
        TestFailingInMemorySource.fail = true;
        TestFailingInMemorySource.connectionCallback.onError(new ConnectionUnavailableException("Connection Lost"));
        Thread.sleep(6000);
        try {
            InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
            AssertJUnit.fail();
        } catch (Throwable e) {
            AssertJUnit.assertTrue(e instanceof SubscriberUnAvailableException);
        }
        TestFailingInMemorySource.fail = false;
        Thread.sleep(10000);
        try {
            InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        } catch (SubscriberUnAvailableException e) {
            AssertJUnit.fail();
        }
        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of errors", 1, TestFailingInMemorySource.numberOfErrorOccurred);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"inMemoryWithFailingSource"})
    public void inMemoryTestCase8() throws InterruptedException {
        log.info("Test inMemory 8");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testEventInMemory', topic='Foo', prop1='hi', prop2='test', fail='true', " +
                "   @map(type='testString', @attributes(symbol='trp:symbol'," +
                "        volume='volume',price='trp:price'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"inMemoryTestCase8"})
    public void inMemoryTestCase9() throws InterruptedException {
        log.info("Test inMemory 9");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testStringInMemory', topic='{{symbol}}', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

    }

    @Test(dependsOnMethods = {"inMemoryTestCase9"})
    public void inMemorySourceSinkAndEventMappingWithSiddhiQLAndRef() throws InterruptedException,
            SubscriberUnAvailableException {
        log.info("Test inMemory Source Sink And EventMapping With SiddhiQL and Ref");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(ref='test1', @map(type='passThrough')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(ref='test2', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("test1.topic", "Foo");
        systemConfigs.put("test1.type", "inMemory");
        systemConfigs.put("test2.type", "inMemory");
        systemConfigs.put("test2.topic", "{{symbol}}");
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(null, systemConfigs);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        InMemoryBroker.publish("IBM", new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemorySourceSinkAndEventMappingWithSiddhiQLAndRef"})
    public void inMemoryTestCase10() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 10");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='Foo', prop1='hi', prop2='7.5', " +
                "   @map(type='testTrp', @attributes(symbol='trp:symbol'," +
                "        volume='2',price='trp:price'))) " +
                "define stream FooStream (symbol string, price double, volume long); " +
                "define stream BarStream (symbol string, price double, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
                for (Event event : events) {
                    AssertJUnit.assertArrayEquals(event.getData(), new Object[]{"hi", 7.5, 100L});
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 5.5, 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"IBM", 5.5, 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 5.5, 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase10"})
    public void inMemoryTestCase11() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 11");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', topic='Foo', prop1='hi', prop2='test', " +
                "   @map(type='testTrp', @attributes('trp:symbol'," +
                "        'trp:price', '2'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        ServiceDeploymentInfo serviceDeploymentInfos = siddhiAppRuntime.getSources().
                iterator().next().get(0).getServiceDeploymentInfo();
        Assert.assertNull(serviceDeploymentInfos);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
                for (Event event : events) {
                    AssertJUnit.assertArrayEquals(event.getData(), new Object[]{"hi", "test", 100L});
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"IBM", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase11"})
    public void inMemoryTestCase12() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 12");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testDepInMemory', topic='Foo', prop1='hi', prop2='test', dep:prop3='foo', " +
                "   @map(type='testTrp', @attributes('trp:symbol'," +
                "        'trp:price', '2'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        ServiceDeploymentInfo serviceDeploymentInfo = siddhiAppRuntime.getSources().iterator().next().get(0)
                .getServiceDeploymentInfo();
        Assert.assertNotNull(serviceDeploymentInfo);
        Assert.assertTrue(serviceDeploymentInfo.isSecured());
        Assert.assertTrue(serviceDeploymentInfo.getServiceProtocol() ==
                ServiceDeploymentInfo.ServiceProtocol.UDP);
        Assert.assertTrue(serviceDeploymentInfo.getPort() == 9000);
        Assert.assertTrue(serviceDeploymentInfo.getDeploymentProperties().get("prop3").equals("foo"));

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
                for (Event event : events) {
                    AssertJUnit.assertArrayEquals(event.getData(), new Object[]{"hi", "test", 100L});
                }
            }
        });
        siddhiAppRuntime.start();
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"IBM", "in", 100L}));
        InMemoryBroker.publish("Foo", new Event(System.currentTimeMillis(), new Object[]{"WSO2", "in", 100L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, wso2Count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"inMemoryTestCase12"})
    public void inMemoryTestCase13() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemory 13");

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory1', topic='Foo', prop1='hi', prop2='test', dep:prop3='foo', " +
                "   @map(type='testTrp', @attributes('trp:symbol'," +
                "        'trp:price', '2'))) " +
                "define stream FooStream (symbol string, price string, volume long); " +
                "define stream BarStream (symbol string, price string, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase13"})
    public void inMemoryTestCase14() throws InterruptedException {
        log.info("Test inMemoryTestCase14");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testDepInMemory', topic='{{symbol}}', dep:prop3='foo', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        ServiceDeploymentInfo serviceDeploymentInfo = (ServiceDeploymentInfo) siddhiAppRuntime.getSinks().
                iterator().next().get(0).getServiceDeploymentInfoList().get(0);
        Assert.assertNotNull(serviceDeploymentInfo);
        Assert.assertTrue(serviceDeploymentInfo.isSecured());
        Assert.assertTrue(serviceDeploymentInfo.getServiceProtocol() ==
                ServiceDeploymentInfo.ServiceProtocol.TCP);
        Assert.assertTrue(serviceDeploymentInfo.getPort() == 8080);
        Assert.assertTrue(serviceDeploymentInfo.getDeploymentProperties().get("prop3").equals("foo"));


        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"inMemoryTestCase14"})
    public void inMemoryTestCase15() throws InterruptedException {

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testInMemory', topic='foo', dep:prop3='foo', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"inMemoryTestCase15"})
    public void inMemoryTestCase16() throws InterruptedException {
        log.info("Test inMemoryTestCase16");


        InMemoryBroker.Subscriber subscription = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "bar-foo";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscription);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testOptionInMemory', topic='foo', @map(type='passThrough')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setAttribute("TopicPrefix", "bar");
        Assert.assertEquals(siddhiManager.getAttributes().get("TopicPrefix"), "bar");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, ibmCount.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscription);
    }


    @Test(dependsOnMethods = {"inMemoryTestCase16"})
    public void inMemoryTestCase17() throws InterruptedException {
        log.info("Test inMemoryTestCase17");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionInMemory = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                int count = ibmCount.incrementAndGet();
                switch (count) {
                    case 1:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "WSO2", "test"});
                        break;
                    case 2:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "IBM", "test"});
                        break;
                    case 3:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "WSO2", "test"});
                        break;
                }
            }

            @Override
            public String getTopic() {
                return "inMemory";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionInMemory);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', prefix='test', @map(type='testSinkOption')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

//        assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 0, wso2Count.get());
        AssertJUnit.assertEquals("Number of InMemory events", 3, ibmCount.get());
        siddhiAppRuntime.shutdown();

//        unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionInMemory);
    }

    @Test(dependsOnMethods = {"inMemoryTestCase17"})
    public void inMemoryTestCase18() throws InterruptedException {
        log.info("Test inMemoryTestCase18");

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                int count = ibmCount.incrementAndGet();
                switch (count) {
                    case 1:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "WSO2", "true-LONG"});
                        break;
                    case 2:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "WSO2", "false-STRING"});
                        break;
                    case 3:
                        Assert.assertEquals(((Event) msg).getData(), new Object[]{"inMemory", "WSO2", "true-LONG"});
                        break;
                }
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', prefix='test', " +
                "@map(type='testSinkOption', mapType='true', @payload(a='volume', b='{{volume}}'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

//        assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        AssertJUnit.assertEquals("Number of InMemory events", 4, ibmCount.get());
        siddhiAppRuntime.shutdown();

//        un-subscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"inMemoryTestCase18"})
    public void inMemoryTestCase19() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemoryTestCase19");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='WSO2', @map(type='testSourceOption')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (wso2Count.incrementAndGet()) {
                        case 1:
                            Assert.assertEquals(event.getData(), new Object[]{"WSO2", "inMemory", 100L});
                            break;
                        case 2:
                            Assert.assertEquals(event.getData(), new Object[]{"WSO2", "inMemory", 101L});
                            break;
                        default:
                            org.testng.AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        InMemoryBroker.publish("WSO2", new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 101L}));
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 2, wso2Count.get());
        siddhiAppRuntime.shutdown();

        InMemoryBroker.unsubscribe(subscriptionWSO2);

    }

    @Test(dependsOnMethods = {"inMemoryTestCase19"})
    public void inMemoryTestCase20() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemoryTestCase20");

        InMemoryBroker.Subscriber subscription = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event[]) {
                    for (Event event : (Event[]) msg) {
                        wso2Count.incrementAndGet();
                    }
                } else {
                    wso2Count.incrementAndGet();
                }
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "out";
            }
        };

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream StockStream (symbol1 string, price1 float, volume1 long); " +
                "@source(type='inMemory', topic='in', @map(type='passThrough')) " +
                "define stream CheckStockStream (symbol1 string, price1 float, volume1 long); " +
                "@sink(type='inMemory', topic='out', @map(type='passThrough')) " +
                "define stream OutputStream (symbol1 string, total double); " +
                "define table StockTable (symbol1 string, price1 float, volume1 long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream as a join StockTable as b " +
                "select b.symbol1, sum(b.price1) as total " +
                "group by b.symbol1 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertEquals(events.length, 4);
                Assert.assertEquals(events[0].getData(1), 120.0);
                Assert.assertEquals(events[1].getData(1), 4.0);
                Assert.assertEquals(events[2].getData(1), 120.0);
                Assert.assertEquals(events[3].getData(1), 4.0);

            }

//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    for (Event event : inEvents) {
//                        inEventCount++;
//                        switch (inEventCount) {
//                            case 1:
//                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2"}, event.getData());
//                                break;
//                            default:
//                                AssertJUnit.assertSame(1, inEventCount);
//                        }
//                    }
//                    eventArrived = true;
//                }
//                if (removeEvents != null) {
//                    removeEventCount = removeEventCount + removeEvents.length;
//                }
//                eventArrived = true;
//            }

        });

        InMemoryBroker.subscribe(subscription);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"IBM", 50.0f, 100L});
        stockStream.send(new Object[]{"IBM", 70.0f, 10L});
        stockStream.send(new Object[]{"WSO2", 1f, 10L});
        stockStream.send(new Object[]{"WSO2", 1f, 10L});
        stockStream.send(new Object[]{"WSO2", 2f, 10L});


        InMemoryBroker.publish("in",
                new Event[]{
                        new Event(System.currentTimeMillis(), new Object[]{"Foo", 55.6f, 200L}),
                        new Event(System.currentTimeMillis(), new Object[]{"Foo", 55.6f, 200L})});
        InMemoryBroker.publish("in",
                new Event[]{
                        new Event(System.currentTimeMillis(), new Object[]{"Foo", 55.6f, 200L}),
                        new Event(System.currentTimeMillis(), new Object[]{"Foo", 55.6f, 200L})});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, wso2Count.get());
        AssertJUnit.assertEquals("Number of events sets", 2, ibmCount.get());

        InMemoryBroker.unsubscribe(subscription);

        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"inMemoryTestCase20"})
    public void inMemoryTestCase21() throws InterruptedException, SubscriberUnAvailableException {
        log.info("Test inMemoryTestCase21");

        InMemoryBroker.Subscriber subscription = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event[]) {
                    for (Event event : (Event[]) msg) {
                        wso2Count.incrementAndGet();
                    }
                } else {
                    wso2Count.incrementAndGet();
                }
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "out";
            }
        };

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream StockStream (symbol1 string, price1 float, volume1 long); " +
                "@source(type='inMemory', topic='in', @map(type='passThrough')) " +
                "define stream CheckStockStream (startTime string, perValue string); " +
                "@sink(type='inMemory', topic='out', @map(type='passThrough')) " +
                "define stream OutputStream (symbol1 string, totalPrice double); " +
                "" +
                "define aggregation StockAggregation " +
                "from StockStream " +
                "select symbol1, avg(price1) as avgPrice, sum(price1) as totalPrice " +
                "group by symbol1 " +
                "aggregate every sec...year; " +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream as a join StockAggregation as b " +
                "within startTime " +
                "per perValue " +
                "select b.symbol1, b.totalPrice " +
                "group by b.symbol1 " +
                "order by b.symbol1 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertEquals(events.length, 4);
                Assert.assertEquals(events[0].getData(1), 120.0);
                Assert.assertEquals(events[1].getData(1), 4.0);
                Assert.assertEquals(events[2].getData(1), 120.0);
                Assert.assertEquals(events[3].getData(1), 4.0);

            }

//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    for (Event event : inEvents) {
//                        inEventCount++;
//                        switch (inEventCount) {
//                            case 1:
//                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2"}, event.getData());
//                                break;
//                            default:
//                                AssertJUnit.assertSame(1, inEventCount);
//                        }
//                    }
//                    eventArrived = true;
//                }
//                if (removeEvents != null) {
//                    removeEventCount = removeEventCount + removeEvents.length;
//                }
//                eventArrived = true;
//            }

        });

        InMemoryBroker.subscribe(subscription);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"IBM", 50.0f, 100L});
        stockStream.send(new Object[]{"IBM", 70.0f, 10L});
        stockStream.send(new Object[]{"WSO2", 1f, 10L});
        stockStream.send(new Object[]{"WSO2", 1f, 10L});
        stockStream.send(new Object[]{"WSO2", 2f, 10L});

        LocalDate currentDate = LocalDate.now();
        String year = String.valueOf(currentDate.getYear());

        InMemoryBroker.publish("in",
                new Event[]{
                        new Event(System.currentTimeMillis(), new Object[]{year + "-**-** **:**:**", "days"}),
                        new Event(System.currentTimeMillis(), new Object[]{year + "-**-** **:**:**", "days"})});
        InMemoryBroker.publish("in",
                new Event[]{
                        new Event(System.currentTimeMillis(), new Object[]{year + "-**-** **:**:**", "days"}),
                        new Event(System.currentTimeMillis(), new Object[]{year + "-**-** **:**:**", "days"})});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, wso2Count.get());
        AssertJUnit.assertEquals("Number of events sets", 2, ibmCount.get());

        InMemoryBroker.unsubscribe(subscription);

        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = {"inMemoryTestCase21"})
    public void inMemoryTestCase22() throws InterruptedException {
        log.info("Test inMemoryTestCase22");
        SiddhiManager siddhiManager = new SiddhiManager();

        String publisherApp = "" +
                "define stream CheckStockStream (symbol1 string, totalPrice double); " +
                "@sink(type='inMemory', topic='OutputStream', @map(type='passThrough')) " +
                "define stream OutputStream (symbol1 string, totalPrice double); " +
                "" +
                "from CheckStockStream " +
                "select * " +
                "insert into OutputStream; ";

        String consumerApp = "" +
                "@source(type='inMemory', topic='OutputStream', @map(type='passThrough')) " +
                "define stream InputStream (symbol1 string, totalPrice double); ";

        SiddhiAppRuntime publisherRuntime = siddhiManager.createSiddhiAppRuntime(publisherApp);
        SiddhiAppRuntime consumerRuntime = siddhiManager.createSiddhiAppRuntime(consumerApp);

        consumerRuntime.addCallback("InputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                wso2Count.incrementAndGet();
            }
        });
        InputHandler stockStream = publisherRuntime.getInputHandler("CheckStockStream");

        publisherRuntime.start();
        consumerRuntime.start();

        stockStream.send(new Object[]{"WSO2", 50.0f});
        stockStream.send(new Object[]{"WSO2", 70.0f});
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                consumerRuntime.getSources().iterator().next().get(0).pause();
            }
        });
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    stockStream.send(new Object[]{"WSO2", 90f});
                } catch (InterruptedException ignored) {
                }
            }
        });
        Thread.sleep(2000);
        consumerRuntime.getSources().iterator().next().get(0).resume();
        Thread.sleep(2000);
        Assert.assertEquals(wso2Count.get(), 3);
        siddhiManager.shutdown();
    }
}

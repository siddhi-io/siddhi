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
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.config.InMemoryConfigManager;
import io.siddhi.core.util.transport.InMemoryBroker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiClientDistributedSinkTestCase {
    private static final Logger log = LogManager.getLogger(MultiClientDistributedSinkTestCase.class);
    private AtomicInteger topic1Count = new AtomicInteger(0);
    private AtomicInteger topic2Count = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        topic1Count.set(0);
        topic2Count.set(0);
    }

    @Test
    public void multiClientRoundRobin() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic1";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testInMemory', @map(type='passThrough'), " +
                "   @distribution(strategy='roundRobin', " +
                "       @destination(topic = 'topic1'), " +
                "       @destination(topic = 'topic2'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        List<ServiceDeploymentInfo> serviceDeploymentInfos = siddhiAppRuntime.getSinks().
                iterator().next().get(0).getServiceDeploymentInfoList();
        Assert.assertEquals(serviceDeploymentInfos.size(), 0);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 3, topic1Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 2, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);

    }

    @Test(dependsOnMethods = {"multiClientRoundRobin"})
    public void singleClientPartitioned() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic1";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testInMemory', @map(type='passThrough'), " +
                "   @distribution(strategy='partitioned', partitionKey='symbol'," +
                "       @destination(topic = 'topic1'), " +
                "       @destination(topic = 'topic2'))) " +
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
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of topic 1 events", 2, topic1Count.get());
        AssertJUnit.assertEquals("Number of topic 2 events", 4, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);

    }

    @Test(dependsOnMethods = {"singleClientPartitioned"})
    public void singleClientBroadcast() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic1";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testInMemory', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(topic = 'topic1'), " +
                "       @destination(topic = 'topic2'))) " +
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
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of topic 1 events", 6, topic1Count.get());
        AssertJUnit.assertEquals("Number of topic 2 events", 6, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
        Thread.sleep(2000);

    }

    @Test(dependsOnMethods = {"singleClientBroadcast"})
    public void singleClientFailingBroadcast() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic3";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic4";
            }
        };

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testFailingInMemory', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(topic = 'topic3'), " +
                "       @destination(topic = 'topic4'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        TestFailingInMemorySink.fail = true;
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        TestFailingInMemorySink.fail = false;
        Thread.sleep(6000);
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of topic 1 events", 5, topic1Count.get());
        AssertJUnit.assertEquals("Number of topic 2 events", 5, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);

    }

    @Test(dependsOnMethods = {"singleClientFailingBroadcast"})
    public void multiClientFailingBroadcast2() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
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
                "@sink(type='testFailingInMemory2', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(topic = 'IBM', test='1'), " +
                "       @destination(topic = 'WSO2',  test='2'))) " +
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
        TestFailingInMemorySink2.failOnce = true;
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        Thread.sleep(6000);
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of topic 1 events", 6, topic1Count.get());
        AssertJUnit.assertEquals("Number of topic 2 events", 5, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);
    }

    @Test(dependsOnMethods = {"multiClientFailingBroadcast2"})
    public void multiClientFailingBroadcast3() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event) {
                    topic1Count.incrementAndGet();
                    return;
                }
                topic1Count.addAndGet(((Event[]) msg).length);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event) {
                    topic2Count.incrementAndGet();
                    return;
                }
                topic2Count.addAndGet(((Event[]) msg).length);
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
                "@sink(type='testFailingInMemory2', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(topic = 'IBM', test='1'), " +
                "       @destination(topic = 'WSO2', test='2'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                        TestFailingInMemorySink2.fail = false;
                    } catch (InterruptedException ignore) {
                    }
                }

            }.start();
            TestFailingInMemorySink2.fail = true;
            stockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(6000);
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

            Thread.sleep(100);

            //assert event count
            AssertJUnit.assertEquals("Number of topic 1 events", 5, topic1Count.get());
            AssertJUnit.assertEquals("Number of topic 2 events", 5, topic2Count.get());
        } finally {
            siddhiAppRuntime.shutdown();

            //unsubscribe from "inMemory" broker per topic
            InMemoryBroker.unsubscribe(subscriptionWSO2);
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }

    @Test(dependsOnMethods = {"multiClientFailingBroadcast3"})
    public void multiClientFailingBroadcast4() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With wait");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event) {
                    topic1Count.incrementAndGet();
                    return;
                }
                topic1Count.addAndGet(((Event[]) msg).length);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                if (msg instanceof Event) {
                    topic2Count.incrementAndGet();
                    return;
                }
                topic2Count.addAndGet(((Event[]) msg).length);
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
                "@sink(type='testFailingInMemory2', on.error='wait', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(topic = 'IBM', test='1'), " +
                "       @destination(topic = 'WSO2', test='2'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                        TestFailingInMemorySink2.fail = false;
                    } catch (InterruptedException ignore) {
                    }
                }

            }.start();
            TestFailingInMemorySink2.fail = true;
            stockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(11000);
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

            Thread.sleep(100);

            //assert event count
            AssertJUnit.assertEquals("Number of topic 1 events", 6, topic1Count.get());
            AssertJUnit.assertEquals("Number of topic 2 events", 6, topic2Count.get());
        } finally {
            siddhiAppRuntime.shutdown();

            //unsubscribe from "inMemory" broker per topic
            InMemoryBroker.unsubscribe(subscriptionWSO2);
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }


    @Test(dependsOnMethods = {"multiClientFailingBroadcast4"})
    public void singleClientBroadcastWithRef() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params with ref");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic1";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(ref='test1', @map(type='passThrough'), " +
                "   @distribution(strategy='broadcast'," +
                "       @destination(ref='test2'), " +
                "       @destination(ref='test3'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("test1.type", "testInMemory");
        systemConfigs.put("test2.topic", "topic1");
        systemConfigs.put("test3.topic", "topic2");
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(null, systemConfigs);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            stockStream.send(new Object[]{"IBM", 57.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

            Thread.sleep(100);

            //assert event count
            AssertJUnit.assertEquals("Number of topic 1 events", 6, topic1Count.get());
            AssertJUnit.assertEquals("Number of topic 2 events", 6, topic2Count.get());
        } finally {

            siddhiAppRuntime.shutdown();

            //unsubscribe from "inMemory" broker per topic
            InMemoryBroker.unsubscribe(subscriptionWSO2);
            InMemoryBroker.unsubscribe(subscriptionIBM);
        }
    }

    @Test(dependsOnMethods = {"singleClientBroadcastWithRef"})
    public void multiClientRoundRobinWithDep() throws InterruptedException {
        log.info("Test inMemorySink And EventMapping With SiddhiQL Dynamic Params");

        InMemoryBroker.Subscriber subscriptionWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic1Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic1";
            }
        };

        InMemoryBroker.Subscriber subscriptionIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                topic2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "topic2";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriptionWSO2);
        InMemoryBroker.subscribe(subscriptionIBM);

        String streams = "" +
                "@app:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='testDepInMemory', dep:foo='bar', @map(type='passThrough'), " +
                "   @distribution(strategy='roundRobin', " +
                "       @destination(topic = 'topic1', dep:foo1='bar1'), " +
                "       @destination(topic = 'topic2', dep:foo2='bar2'))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        List<ServiceDeploymentInfo> serviceDeploymentInfos = siddhiAppRuntime.getSinks().
                iterator().next().get(0).getServiceDeploymentInfoList();
        Assert.assertEquals(serviceDeploymentInfos.size(), 2);
        for (int i = 0; i < serviceDeploymentInfos.size(); i++) {
            ServiceDeploymentInfo serviceDeploymentInfo = serviceDeploymentInfos.get(i);
            Assert.assertNotNull(serviceDeploymentInfo);
            Assert.assertTrue(serviceDeploymentInfo.isSecured());
            Assert.assertTrue(serviceDeploymentInfo.getServiceProtocol() ==
                    ServiceDeploymentInfo.ServiceProtocol.TCP);
            Assert.assertTrue(serviceDeploymentInfo.getPort() == 8080);
            Assert.assertTrue(serviceDeploymentInfo.getDeploymentProperties().get("foo").equals("bar"));
            if (i == 0) {
                Assert.assertTrue(serviceDeploymentInfo.getDeploymentProperties().get("foo1").equals("bar1"));
            } else {
                Assert.assertTrue(serviceDeploymentInfo.getDeploymentProperties().get("foo2").equals("bar2"));
            }
        }
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Number of WSO2 events", 3, topic1Count.get());
        AssertJUnit.assertEquals("Number of IBM events", 2, topic2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriptionWSO2);
        InMemoryBroker.unsubscribe(subscriptionIBM);

    }
}

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
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncTestCase {
    private static final Logger log = LogManager.getLogger(AsyncTestCase.class);
    private AtomicInteger count;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger();
        eventArrived = false;
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void asyncTest1() throws InterruptedException {
        log.info("async test 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:async " +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into outputStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from cseEventStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


    }


    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"asyncTest1"})
    public void asyncTest2() throws InterruptedException {
        log.info("async test 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:async(buffer.size='2')" +
                " " +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

    }

    @Test(dependsOnMethods = {"asyncTest2"})
    public void asyncTest3() throws InterruptedException {
        log.info("async test 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='2')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                try {
                    Thread.sleep(Math.round(Math.random()) * 1000 + 1000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                eventArrived = true;
                for (Event event : events) {
                    count.incrementAndGet();
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long startTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{"WSO2", 55.6f, 100});
        inputHandler.send(new Object[]{"IBM", 9.6f, 100});
        inputHandler.send(new Object[]{"FB", 7.6f, 100});
        inputHandler.send(new Object[]{"GOOG", 5.6f, 100});
        inputHandler.send(new Object[]{"WSO2", 15.6f, 100});
        long timeDiff = System.currentTimeMillis() - startTime;
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(5, count.get());
        AssertJUnit.assertTrue(timeDiff >= 2000);

    }

    @Test(dependsOnMethods = {"asyncTest3"})
    public void asyncTest4() throws InterruptedException {
        log.info("async test 4");
        HashMap<String, Integer> threads = new HashMap<>();
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='16', workers='2', batch.size.max='2')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 < price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                try {
                    Thread.sleep(Math.round(Math.random()) * 1000 + 1000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                eventArrived = true;
                for (Event event : events) {
                    count.incrementAndGet();
                }
                Assert.assertTrue(events.length <= 2);
                synchronized (threads) {
                    Integer count = threads.get(Thread.currentThread().getName());
                    if (count == null) {
                        threads.put(Thread.currentThread().getName(), 1);
                    } else {
                        count++;
                        threads.put(Thread.currentThread().getName(), count);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        for (int i = 0; i < 20; i++) {
            inputHandler.send(new Object[]{"WSO2", 115.6f, 100 + i});
        }
        SiddhiTestHelper.waitForEvents(2000, 20, count, 10000);
        AssertJUnit.assertEquals(20, count.get());
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(eventArrived);
        log.info("Threads count:" + threads.size() + " threads:" + threads);
        Assert.assertEquals(threads.size(), 2);

    }

    @Test//(dependsOnMethods = {"asyncTest4"})
    public void asyncTest5() throws InterruptedException {
        log.info("async test 5");
        HashMap<String, Integer> threads = new HashMap<>();
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='512', workers='10', batch.size.max='20')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 < price] " +
                "select * " +
                "insert into outputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                try {
                    Thread.sleep(Math.round(Math.random()) * 1000 + 1000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                eventArrived = true;
                for (Event event : events) {
                    count.incrementAndGet();
                }
                Assert.assertTrue(events.length <= 20);
                synchronized (threads) {
                    Integer count = threads.get(Thread.currentThread().getName());
                    if (count == null) {
                        threads.put(Thread.currentThread().getName(), 1);
                    } else {
                        count++;
                        threads.put(Thread.currentThread().getName(), count);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1200; i++) {
            inputHandler.send(new Object[]{"WSO2", 115.6f, 100 + i});
        }
        SiddhiTestHelper.waitForEvents(3000, 1200, count, 10000);
        long timeDiff = System.currentTimeMillis() - startTime;
        log.info("Time spent: " + timeDiff);
        AssertJUnit.assertEquals(1200, count.get());
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(eventArrived);
        log.info("Threads count:" + threads.size() + " threads:" + threads);
        Assert.assertEquals(threads.size(), 10);
    }

    @Test(dependsOnMethods = {"asyncTest5"})
    public void asyncTest6() throws InterruptedException {
        log.info("async test 6");
        HashMap<String, Integer> threads = new HashMap<>();
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='16', workers='2', batch.size.max='25')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 < price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                try {
                    Thread.sleep(Math.round(Math.random()) * 1000 + 1000);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                eventArrived = true;
                for (Event event : events) {
                    count.incrementAndGet();
                }
                Assert.assertTrue(events.length <= 25);
                synchronized (threads) {
                    Integer count = threads.get(Thread.currentThread().getName());
                    if (count == null) {
                        threads.put(Thread.currentThread().getName(), 1);
                    } else {
                        count++;
                        threads.put(Thread.currentThread().getName(), count);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        for (int i = 0; i < 20; i++) {
            Thread.sleep(100);
            inputHandler.send(new Object[]{"WSO2", 115.6f, 100 + i});
        }
        SiddhiTestHelper.waitForEvents(2000, 20, count, 10000);
        AssertJUnit.assertEquals(20, count.get());
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(eventArrived);
        log.info("Threads count:" + threads.size() + " threads:" + threads);
        Assert.assertEquals(threads.size(), 2);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"asyncTest6"})
    public void asyncTest7() throws InterruptedException {
        log.info("async test 7");

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='16', workers='0', batch.size.max='25')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"asyncTest7"})
    public void asyncTest8() throws InterruptedException {
        log.info("async test 8");

        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                " " +
                "@async(buffer.size='16', workers='1', batch.size.max='0')" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into innerStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from innerStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

    }
}

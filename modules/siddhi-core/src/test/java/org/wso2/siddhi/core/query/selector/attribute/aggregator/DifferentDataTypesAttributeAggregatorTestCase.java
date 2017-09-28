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

package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.test.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class DifferentDataTypesAttributeAggregatorTestCase {

    static final Logger log = Logger.getLogger(DifferentDataTypesAttributeAggregatorTestCase.class);
    private AtomicInteger atomicEventCount;

    @Before
    public void init() {
        atomicEventCount = new AtomicInteger(0);
    }

    @Test
    public void AvgAggregatorDoubleTest() throws InterruptedException {
        log.info("AvgAggregator Test #1: double");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select avg(price) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(0.0, inEvents[0].getData(0));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(3.0, inEvents[0].getData(0));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 0.0});
        inputHandler.send(new Object[]{"WSO2", 0.0});
        inputHandler.send(new Object[]{"WSO2", 3.0});
        inputHandler.send(new Object[]{"WSO2", 3.0});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test
    public void AvgAggregatorDoubleTest2() throws InterruptedException {
        log.info("AvgAggregator Test #2: long");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select avg(price) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(0.0, inEvents[0].getData(0));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(3.0, inEvents[0].getData(0));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 0L});
        inputHandler.send(new Object[]{"WSO2", 0L});
        inputHandler.send(new Object[]{"WSO2", 3L});
        inputHandler.send(new Object[]{"WSO2", 3L});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void AvgAggregatorDoubleTest3() throws InterruptedException {
        log.info("AvgAggregator Test #3: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select avg(price, symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void AvgAggregatorDoubleTest4() throws InterruptedException {
        log.info("AvgAggregator Test #4: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select avg(symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);
    }

    @Test
    public void AvgAggregatorDoubleTest5() throws InterruptedException {
        log.info("AvgAggregator Test #5: float");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price float);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select avg(price) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(0.0, inEvents[0].getData(0));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(3.0, inEvents[0].getData(0));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 0F});
        inputHandler.send(new Object[]{"WSO2", 0F});
        inputHandler.send(new Object[]{"WSO2", 3F});
        inputHandler.send(new Object[]{"WSO2", 3F});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test
    public void MaxAggregatorDoubleTest6() throws InterruptedException {
        log.info("MaxAggregator Test #6: double");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select max(price) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(0.3, inEvents[0].getData(0));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(3.3, inEvents[0].getData(0));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 0.1});
        inputHandler.send(new Object[]{"WSO2", 0.3});
        inputHandler.send(new Object[]{"WSO2", 3.1});
        inputHandler.send(new Object[]{"WSO2", 3.3});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void MaxAggregatorDoubleTest7() throws InterruptedException {
        log.info("MaxAggregator Test #7: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select max(price, symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void MaxAggregatorDoubleTest8() throws InterruptedException {
        log.info("MaxAggregator Test #8: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select max(symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

    }

    @Test
    public void MaxAggregatorDoubleTest9() throws InterruptedException {
        log.info("MaxAggregator Test #9: long");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price long, quantity int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select max(price) as maxPrice, max(quantity) as maxQuantity " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(3L, inEvents[0].getData(0));
                    Assert.assertEquals(3, inEvents[0].getData(1));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(5L, inEvents[0].getData(0));
                    Assert.assertEquals(5, inEvents[0].getData(1));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 1L ,1});
        inputHandler.send(new Object[]{"WSO2", 3L, 3});
        inputHandler.send(new Object[]{"WSO2", 3L, 3});
        inputHandler.send(new Object[]{"WSO2", 5L, 5});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test
    public void MinAggregatorDoubleTest10() throws InterruptedException {
        log.info("MinAggregator Test #9: int, long, double, float");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price long, quantity int, id float, rating double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select min(price) as minPrice, min(quantity) as minQuantity, min(id) as minId, min(rating) as "
                + "minRating "
                + " " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

        execPlanRunTime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                atomicEventCount.addAndGet(inEvents.length);
                if (atomicEventCount.get() == 1) {
                    Assert.assertEquals(1L, inEvents[0].getData(0));
                    Assert.assertEquals(1, inEvents[0].getData(1));
                    Assert.assertEquals(1.0F, inEvents[0].getData(2));
                    Assert.assertEquals(1.1, inEvents[0].getData(3));
                } else if (atomicEventCount.get() == 2) {
                    Assert.assertEquals(3L, inEvents[0].getData(0));
                    Assert.assertEquals(3, inEvents[0].getData(1));
                    Assert.assertEquals(3.0F, inEvents[0].getData(2));
                    Assert.assertEquals(3.3, inEvents[0].getData(3));
                }
            }
        });

        execPlanRunTime.start();
        InputHandler inputHandler = execPlanRunTime.getInputHandler("cseEventStream");
        inputHandler.send(new Object[]{"WSO2", 1L ,1, 1F, 1.1});
        inputHandler.send(new Object[]{"WSO2", 3L, 3, 3F, 3.3});
        inputHandler.send(new Object[]{"WSO2", 3L, 3, 3F, 3.3});
        inputHandler.send(new Object[]{"WSO2", 5L, 5, 5F, 5.5});
        SiddhiTestHelper.waitForEvents(100, 2, atomicEventCount, 2000);
        execPlanRunTime.shutdown();
        Assert.assertEquals(2, atomicEventCount.intValue());
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void MinAggregatorDoubleTest11() throws InterruptedException {
        log.info("MinAggregator Test #11: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select min(price, symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void MinAggregatorDoubleTest12() throws InterruptedException {
        log.info("MinAggregator Test #12: validation");

        SiddhiManager siddhiManager = new SiddhiManager();

        String execPlan = "" +
                "@Plan:name('AvgAggregatorTests') " +
                "" +
                "define stream cseEventStream (symbol string, price double);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(2) " +
                "select min(symbol) as average " +
                "group by symbol " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime execPlanRunTime = siddhiManager.createExecutionPlanRuntime(execPlan);

    }


}

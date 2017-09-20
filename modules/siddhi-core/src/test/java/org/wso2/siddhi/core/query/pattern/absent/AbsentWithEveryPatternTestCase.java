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

package org.wso2.siddhi.core.query.pattern.absent;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.TestUtil;
import org.wso2.siddhi.core.stream.input.InputHandler;

/**
 * Test the patterns:
 * - every A -> not B for 1 sec
 * - not A for 1 sec -> every B
 */
public class AbsentWithEveryPatternTestCase {

    private static final Logger log = Logger.getLogger(AbsentWithEveryPatternTestCase.class);

    @Test
    public void testQuery1() throws InterruptedException {
        log.info("Test the query every e1 -> not e2 for 1 sec with e1 only");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price1 float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price1>e1.price] for 1sec " +
                "select e1.symbol as symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1", new Object[]{"WSO2"},
                new Object[]{"GOOG"});

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(1100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 2, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertTrue("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery2() throws InterruptedException {
        log.info("Test the query every e1 -> not e2 for 1 sec with e1 and e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price1 float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price1>e1.price] for 1sec " +
                "select e1.symbol as symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1");

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(1100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 0, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertFalse("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testQuery3() throws InterruptedException {
        log.info("Test the query every e1 -> not e2 for 1 sec -> e3 with e1 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price>e1.price] for 1sec -> " +
                "e3=Stream3[price>e1.price] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1", new Object[]{"WSO2",
                "IBM"}, new Object[]{"GOOG", "IBM"});

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream3 = siddhiAppRuntime.getInputHandler("Stream3");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(1100);
        stream3.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 2, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertTrue("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery4() throws InterruptedException {
        log.info("Test the query not e1 for 1 sec -> every e2 with e2 only");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] for 1sec -> every e2=Stream2[price>20] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1", new Object[]{"WSO2"},
                new Object[]{"GOOG"});

        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        Thread.sleep(1100);
        stream2.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 2, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertTrue("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery5() throws InterruptedException {
        log.info("Test the query not e1 for 1 sec -> every e2 with e1 and e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>10] for 1sec -> every e2=Stream2[price>20] " +
                "select e2.symbol as symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1");

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 0, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertFalse("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testQuery6() throws InterruptedException {
        log.info("Test the query e1 -> not e2 for 1 sec -> every e3 with e1 and e3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] for 1sec -> " +
                "every e3=Stream3[price>e1.price] " +
                "select e1.symbol as symbol1, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        TestUtil.TestCallback callback = TestUtil.addQueryCallback(siddhiAppRuntime, "query1", new Object[]{"WSO2",
                "GOOG"}, new Object[]{"WSO2", "IBM"});

        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
        InputHandler stream3 = siddhiAppRuntime.getInputHandler("Stream3");

        siddhiAppRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(1100);
        stream3.send(new Object[]{"GOOG", 55.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"IBM", 55.8f, 100});
        Thread.sleep(100);

        callback.throwAssertionErrors();
        AssertJUnit.assertEquals("Number of success events", 2, callback.getInEventCount());
        AssertJUnit.assertEquals("Number of remove events", 0, callback.getRemoveEventCount());
        AssertJUnit.assertTrue("Event arrived", callback.isEventArrived());

        siddhiAppRuntime.shutdown();
    }
}

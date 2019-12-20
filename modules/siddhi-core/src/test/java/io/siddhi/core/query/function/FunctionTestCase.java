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

package io.siddhi.core.query.function;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FunctionTestCase {

    private static final Logger log = Logger.getLogger(FunctionTestCase.class);
    private int count;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {

        count = 0;
        eventArrived = false;
    }

    //Coalesce

    @Test
    public void functionTest1() throws InterruptedException {

        log.info("function test 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price1", Attribute.Type.FLOAT).attribute("price2", Attribute.Type.FLOAT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.function("coalesce", Expression.variable("price1"), Expression
                                .variable("price2")))
        );
        query.insertInto("StockQuote");

        SiddhiApp siddhiApp = new SiddhiApp("ep1");
        siddhiApp.defineStream(cseEventStream);
        siddhiApp.addQuery(query);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(55.6f, inEvent.getData()[1]);
                    } else if (count == 2) {
                        AssertJUnit.assertEquals(65.7f, inEvent.getData()[1]);
                    } else if (count == 3) {
                        AssertJUnit.assertEquals(23.6f, inEvent.getData()[1]);
                    } else if (count == 4) {
                        AssertJUnit.assertEquals(34.6f, inEvent.getData()[1]);
                    } else if (count == 5) {
                        AssertJUnit.assertNull(inEvent.getData()[1]);
                    }
                }
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 55.6f, 70.6f});
        inputHandler.send(new Object[]{"WSO2", 65.7f, 12.8f});
        inputHandler.send(new Object[]{"WSO2", 23.6f, null});
        inputHandler.send(new Object[]{"WSO2", null, 34.6f});
        inputHandler.send(new Object[]{"WSO2", null, null});
        Thread.sleep(100);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void functionTest2() throws InterruptedException {

        log.info("function test 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price1 double, price2 float, volume " +
                "long , quantity int);";
        String query = "@info(name = 'query1') from cseEventStream select symbol, coalesce(price1,price2) as price," +
                "quantity insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(50.0f, inEvent.getData()[1]);
                    } else if (count == 2) {
                        AssertJUnit.assertEquals(70.0f, inEvent.getData()[1]);
                    } else if (count == 3) {
                        AssertJUnit.assertEquals(44.0f, inEvent.getData()[1]);
                    } else if (count == 4) {
                        AssertJUnit.assertEquals(null, inEvent.getData()[1]);
                    }
                }
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 50f, 60f, 60L, 6});
        inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        inputHandler.send(new Object[]{"WSO2", null, 44f, 200L, 56});
        inputHandler.send(new Object[]{"WSO2", null, null, 200L, 56});
        Thread.sleep(100);
        AssertJUnit.assertEquals(4, count);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void functionTest3() throws InterruptedException {

        log.info("function test 3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price1 float, price2 float, volume long" +
                " , quantity int);";
        String query = "@info(name = 'query1') from cseEventStream[coalesce(price1,price2) > 0f] select symbol, " +
                "coalesce(price1,price2) as price,quantity insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(50.0f, inEvent.getData()[1]);
                    } else if (count == 2) {
                        AssertJUnit.assertEquals(70.0f, inEvent.getData()[1]);
                    } else if (count == 3) {
                        AssertJUnit.assertEquals(44.0f, inEvent.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 50f, 60f, 60L, 6});
        inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        inputHandler.send(new Object[]{"WSO2", null, 44f, 200L, 56});
        inputHandler.send(new Object[]{"WSO2", null, null, 200L, 56});
        Thread.sleep(100);
        org.testng.AssertJUnit.assertEquals(3, count);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void functionTest4() throws InterruptedException {

        log.info("function test 4");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price1 float, price2 float, volume long" +
                " , quantity int);";
        String query = "@info(name = 'query1') from cseEventStream[coalesce(price1,price2) > 0f] select symbol, " +
                "coalesce() as price,quantity insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timestamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(50.0f, inEvent.getData()[1]);
                    } else if (count == 2) {
                        AssertJUnit.assertEquals(70.0f, inEvent.getData()[1]);
                    } else if (count == 3) {
                        AssertJUnit.assertEquals(44.0f, inEvent.getData()[1]);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 50f, 60f, 60L, 6});
        inputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        inputHandler.send(new Object[]{"WSO2", null, 44f, 200L, 56});
        inputHandler.send(new Object[]{"WSO2", null, null, 200L, 56});
        Thread.sleep(100);
        org.testng.AssertJUnit.assertEquals(3, count);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery5() throws InterruptedException {

        log.info("Default function test");

        SiddhiManager siddhiManager = new SiddhiManager();

        String planName = "@app:name('DefaultFunction') ";
        String cseEventStream = "define stream cseEventStream (temp double, roomNo int,deviceId long);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream " +
                "select default(temp,0.0,deviceId) as temp, roomNo " +
                "insert into StandardTempStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery6() throws InterruptedException {

        log.info("Default function test for different type");

        SiddhiManager siddhiManager = new SiddhiManager();

        String planName = "@app:name('DefaultFunction') ";
        String cseEventStream = "define stream cseEventStream (temp double, roomNo int,deviceId long);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream " +
                "select default(temp,123) as temp, roomNo " +
                "insert into StandardTempStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery7() throws InterruptedException {

        log.info("eventTimestamp Test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String planName = "@app:name('eventTimestamp') ";
        String cseEventStream = "define stream fooStream (symbol string, time string);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select symbol as name, eventTimestamp(time) as eventTimestamp " +
                "insert into barStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery8() throws InterruptedException {

        log.info("CurrentTimestamp Test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String planName = "@app:name('eventTimestamp') ";
        String cseEventStream = "define stream fooStream (symbol string, currentTime string);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select symbol as name, currentTimeMillis(currentTime) as eventTimestamp " +
                "insert into barStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery9() throws InterruptedException {
        log.info("CreateSet Test1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol string, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol, deviceId) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery10() throws InterruptedException {
        log.info("CreateSet Test2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol int, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery11() throws InterruptedException {
        log.info("CreateSet Test3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol double, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";

        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery12() throws InterruptedException {
        log.info("CreateSet Test4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol bool, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery13() throws InterruptedException {
        log.info("CreateSet Test5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol long, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery14() throws InterruptedException {
        log.info("CreateSet Test6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol float, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test
    public void testFunctionQuery15() throws InterruptedException {
        log.info("CreateSet Test7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('createSet') ";
        String cseEventStream = "define stream fooStream (symbol string, deviceId long);";
        String query = "@info(name = 'query1') " +
                "from fooStream " +
                "select createSet(symbol) as initialSet  " +
                "insert into barStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery16() throws InterruptedException {
        log.info("CreateSet8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('DefaultFunction') ";
        String cseEventStream = "define stream cseEventStream (temp double, roomNo int,deviceId long);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream " +
                "select default(temp,1/2) as temp, roomNo " +
                "insert into StandardTempStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery17() throws InterruptedException {
        log.info("SizeOfSet");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('SizeOfSetFunction') ";
        String cseEventStream = "define stream cseEventStream (distinctSymbols string, roomNo int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream " +
                "select sizeOfSet(distinctSymbols) as sizeOfSymbolSet " +
                "insert into StandardTempStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testFunctionQuery18() throws InterruptedException {
        log.info("SizeOfSet");
        SiddhiManager siddhiManager = new SiddhiManager();
        String planName = "@app:name('SizeOfSetFunction') ";
        String cseEventStream = "define stream cseEventStream (distinctSymbols string, roomNo int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream " +
                "select sizeOfSet(distinctSymbols, roomNo) as sizeOfSymbolSet " +
                "insert into StandardTempStream;";
        siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
    }
}

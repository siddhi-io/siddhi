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

package io.siddhi.core.query;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class PassThroughTestCase {
    private static final Logger log = LogManager.getLogger(PassThroughTestCase.class);
    private int count;
    private AtomicBoolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = new AtomicBoolean(false);
    }


    @Test
    public void passThroughTest1() throws InterruptedException {
        log.info("pass through test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.insertInto("StockQuote");


        SiddhiApp siddhiApp = new SiddhiApp("ep1");
        siddhiApp.defineStream(cseEventStream);
        siddhiApp.addQuery(query);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                AssertJUnit.assertTrue("IBM".equals(inEvents[0].getData(0)) || "WSO2".equals(inEvents[0].getData(0)));
                count += inEvents.length;
                eventArrived.set(true);
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 100);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void passThroughTest2() throws InterruptedException {
        log.info("pass through test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT);
        StreamDefinition cseEventStream1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.insertInto("StockQuote");


        SiddhiApp siddhiApp = new SiddhiApp("ep1");
        siddhiApp.defineStream(cseEventStream);
        siddhiApp.defineStream(cseEventStream1);
        siddhiApp.addQuery(query);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                count += inEvents.length;
                eventArrived.set(true);
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream1");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 100);
        AssertJUnit.assertEquals(0, count);
        AssertJUnit.assertFalse(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void passThroughTest3() throws InterruptedException {
        log.info("pass through test3");
        SiddhiManager siddhiManager = new SiddhiManager();

        StreamDefinition cseEventStream = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT);
        StreamDefinition cseEventStream1 = StreamDefinition.id("cseEventStream1").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT);

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("symbol"))
        );
        query.insertInto("StockQuote");


        SiddhiApp siddhiApp = new SiddhiApp("ep1");
        siddhiApp.defineStream(cseEventStream);
        siddhiApp.defineStream(cseEventStream1);
        siddhiApp.addQuery(query);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
                count += inEvents.length;
                eventArrived.set(true);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("cseEventStream1");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100});
        inputHandler.send(new Object[]{"WSO2", 100});

        inputHandler1.send(new Object[]{"ORACLE", 100});
        inputHandler1.send(new Object[]{"ABC", 100});

        SiddhiTestHelper.waitForEvents(10, eventArrived, 100);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void passThroughTest4() throws InterruptedException {
        log.info("pass through test4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('passThroughTest4') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "insert into outputStream;" +
                "" +
                "@info(name = 'query2') " +
                "from outputStream " +
                "select * " +
                "insert into outputStream2 ;";


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        log.info("Running : " + siddhiAppRuntime.getName());

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timestamp, inEvents, removeEvents);
                AssertJUnit.assertTrue("WSO2".equals(inEvents[0].getData(0)));
                count = count + inEvents.length;
                eventArrived.set(true);
            }

        };
        siddhiAppRuntime.addCallback("query2", queryCallback);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");

        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"WSO2", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        SiddhiTestHelper.waitForEvents(10, eventArrived, 100);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived.get());

        siddhiAppRuntime.shutdown();
    }

}

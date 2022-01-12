/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SandboxTestCase {
    private static final Logger log = LogManager.getLogger(SandboxTestCase.class);
    private AtomicInteger inEventCount;
    private AtomicInteger removeEventCount;
    private boolean eventArrived;
    private AtomicInteger count;

    @BeforeMethod
    public void init() {
        count = new AtomicInteger(0);
        inEventCount = new AtomicInteger(0);
        removeEventCount = new AtomicInteger(0);
        eventArrived = false;
    }

    @Test
    public void sandboxTest1() throws InterruptedException {
        log.info("sandbox test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@source(type='foo')" +
                "@source(type='foo1')" +
                "@sink(type='foo1')" +
                "@source(type='inMemory', topic='myTopic')" +
                "define stream StockStream (symbol string, price float, vol long);\n" +
                "" +
                "@sink(type='foo1')" +
                "@sink(type='inMemory', topic='myTopic1')" +
                "define stream DeleteStockStream (symbol string, price float, vol long);\n" +
                "" +
                "@store(type='rdbms')" +
                "define table StockTable (symbol string, price float, volume long);\n" +
                "" +
                "define stream CountStockStream (symbol string);\n" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol, price, vol as volume " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream[vol>=100] " +
                "delete StockTable " +
                "   on StockTable.symbol==symbol ;" +
                "" +
                "@info(name = 'query3') " +
                "from CountStockStream#window.length(0) join StockTable" +
                " on CountStockStream.symbol==StockTable.symbol " +
                "select CountStockStream.symbol as symbol " +
                "insert into CountResultsStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSandboxSiddhiAppRuntime(app);

        Assert.assertEquals(siddhiAppRuntime.getSources().size(), 1);
        Assert.assertEquals(siddhiAppRuntime.getSinks().size(), 1);
        Assert.assertEquals(siddhiAppRuntime.getTables().size(), 1);

        for (List<Source> sources : siddhiAppRuntime.getSources()) {
            for (Source source : sources) {
                Assert.assertTrue(source.getType().equalsIgnoreCase("inMemory"));
            }
        }

        for (List<Sink> sinks : siddhiAppRuntime.getSinks()) {
            for (Sink sink : sinks) {
                Assert.assertTrue(sink.getType().equalsIgnoreCase("inMemory"));
            }
        }

        for (Table table : siddhiAppRuntime.getTables()) {
            Assert.assertTrue(table instanceof InMemoryTable);
        }

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        InputHandler countStockStream = siddhiAppRuntime.getInputHandler("CountStockStream");

        siddhiAppRuntime.addCallback("CountResultsStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6f, 100L});
        countStockStream.send(new Object[]{"WSO2"});

        Thread.sleep(500);
        Assert.assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void sandboxTest2() throws InterruptedException {
        log.info("sandbox test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "@source(type='foo')" +
                "@source(type='foo1')" +
                "@sink(type='foo1')" +
                "@source(type='inMemory', topic='myTopic')" +
                "define stream StockStream (symbol string, price float, vol long);\n" +
                "" +
                "@sink(type='foo1')" +
                "@sink(type='inMemory', topic='myTopic1')" +
                "define stream DeleteStockStream (symbol string, price float, vol long);\n" +
                "" +
                "@store(type='rdbms')" +
                "define table StockTable (symbol string, price float, volume long);\n" +
                "" +
                "define stream CountStockStream (symbol string);\n" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol, price, vol as volume " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream[vol>=100] " +
                "delete StockTable " +
                "   on StockTable.symbol==symbol ;" +
                "" +
                "@info(name = 'query3') " +
                "from CountStockStream#window.length(0) join StockTable" +
                " on CountStockStream.symbol==StockTable.symbol " +
                "select CountStockStream.symbol as symbol " +
                "insert into CountResultsStream ;";

        SiddhiApp siddhiApp = SiddhiCompiler.parse(app);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSandboxSiddhiAppRuntime(siddhiApp);

        Assert.assertEquals(siddhiAppRuntime.getSources().size(), 1);
        Assert.assertEquals(siddhiAppRuntime.getSinks().size(), 1);
        Assert.assertEquals(siddhiAppRuntime.getTables().size(), 1);

        for (List<Source> sources : siddhiAppRuntime.getSources()) {
            for (Source source : sources) {
                Assert.assertTrue(source.getType().equalsIgnoreCase("inMemory"));
            }
        }

        for (List<Sink> sinks : siddhiAppRuntime.getSinks()) {
            for (Sink sink : sinks) {
                Assert.assertTrue(sink.getType().equalsIgnoreCase("inMemory"));
            }
        }

        for (Table table : siddhiAppRuntime.getTables()) {
            Assert.assertTrue(table instanceof InMemoryTable);
        }

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        InputHandler countStockStream = siddhiAppRuntime.getInputHandler("CountStockStream");

        siddhiAppRuntime.addCallback("CountResultsStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6f, 100L});
        countStockStream.send(new Object[]{"WSO2"});

        Thread.sleep(500);
        Assert.assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();

    }
}

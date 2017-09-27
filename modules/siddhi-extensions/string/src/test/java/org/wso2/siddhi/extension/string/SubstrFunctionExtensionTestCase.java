/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.string;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.string.test.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class SubstrFunctionExtensionTestCase {
    static final Logger log = Logger.getLogger(SubstrFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    /*
    * Test Case for str:substr(<string sourceText> , <int beginIndex>)
    * */
    @Test
    public void testSubstrFunctionExtension1() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase for str:substr(<string sourceText> , <int beginIndex>)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, 4) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals("efghiJ KLMN", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("yut", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("o", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"AbCDefghiJ KLMN", 700f, 100l});
        inputHandler.send(new Object[]{" ertyut", 60.5f, 200l});
        inputHandler.send(new Object[]{"Hello", 60.5f, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    /*
   * Test Case for str:substr(<string sourceText> , <int beginIndex>, <int length>)
   * */
    @Test
    public void testSubstrFunctionExtension2() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase for str:substr(<string sourceText> , <int beginIndex>, "
                         + "<int length>)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , "
                + "str:substr(symbol, 2, 4) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals("CDef", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("rtyu", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("lloo", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"AbCDefghiJ KLMN", 700f, 100l});
        inputHandler.send(new Object[]{" ertyut", 60.5f, 200l});
        inputHandler.send(new Object[]{"Helloooo", 60.5f, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    /*
   * Test Case for str:substr(<string sourceText> , <string regex>)
   * */
    @Test
    public void testSubstrFunctionExtension3() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase for str:substr(<string sourceText> , <string regex>)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, '^WSO2(.*)') " +
                "as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals("WSO2D efghiJ KLMN", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"WSO2D efghiJ KLMN", 700f, 100l});
        inputHandler.send(new Object[]{" ertWSO2yut", 60.5f, 200l});
        inputHandler.send(new Object[]{"Helloooo", 60.5f, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    /*
   * Test Case for str:substr(<string sourceText> , <string regex>, <int groupNumber>)
   * */
    @Test
    public void testSubstrFunctionExtension4() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase for str:substr(<string sourceText> , <string regex>, " +
                         "<int groupNumber>)");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, " +
                "'WSO2(.*)A(.*)', 2) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(" ello", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("o", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("llo", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"hello hi hWSO2 hiA ello", 700f, 100l});
        inputHandler.send(new Object[]{"WSO2 hiA WSO2 hellAo", 60.5f, 200l});
        inputHandler.send(new Object[]{"WSO2 cep WSO2 XX E hi hA WSO2 heAllo", 60.5f, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test
    public void testSubstrFunctionExtension5() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, " +
                "price, 2) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(" ello", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("o", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("llo", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"hello hi hWSO2 hiA ello", "WSO2(.*)A(.*)", 100l});
        inputHandler.send(new Object[]{"WSO2 hiA WSO2Ao", "(.*)WSO2A(.*)", 200l});
        inputHandler.send(new Object[]{"eAllo", "(.*)A(.*)", 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testSubstrFunctionExtension6() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, " +
                "price) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals("WSO2 hiA ello", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals("WSO2 hiA WSO2Ao", event.getData(1));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals("eAllo", event.getData(1));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"hello hi hWSO2 hiA ello", "WSO2(.*)A(.*)", 100l});
        inputHandler.send(new Object[]{"WSO2 hiA WSO2Ao", "(.*)WSO2A(.*)", 200l});
        inputHandler.send(new Object[]{"eAllo", "(.*)A(.*)", 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test(expected = ExecutionPlanValidationException.class)
    public void testSubstrFunctionExtensionWithInvalidDataType() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(price, 4) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testSubstrFunctionExtensionWithInvalidDataType1() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "str:substr(symbol, volume) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testSubstrFunctionExtensionWithInvalidDataType2() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "str:substr(symbol, 2, volume) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.
                createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testSubstrFunctionExtensionWithInvalidDataType3() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol, volume, 1) " +
                "as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testSubstrFunctionExtensionWithOneArgument() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.start();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testSubstrFunctionExtensionWithNullValues() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase with  null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select symbol , str:substr(symbol,2) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{null, 700f, 100l, 1});
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testSubstrFunctionExtensionWithNullValues1() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase with null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, volume long, count int);";
        String query = ("@info(name = 'query1') from inputStream select symbol , " +
                "str:substr(symbol,count) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 100l, null});
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testSubstrFunctionExtensionWithNullValues3() throws InterruptedException {
        log.info("SubstrFunctionExtension TestCase with  null value");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol1 string, symbol2 string, " +
                "volume long, count int);";
        String query = ("@info(name = 'query1') from inputStream select symbol1 , " +
                "str:substr(symbol1, symbol2, count) as substring " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", "IBM", 100l, null});
        executionPlanRuntime.shutdown();
    }
}

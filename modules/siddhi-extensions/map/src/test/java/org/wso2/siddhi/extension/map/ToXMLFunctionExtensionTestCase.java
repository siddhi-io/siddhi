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

package org.wso2.siddhi.extension.map;

import junit.framework.Assert;
import org.apache.axiom.om.impl.exception.XMLComparisonException;
import org.apache.axiom.om.impl.llom.util.XMLComparator;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.map.test.util.SiddhiTestHelper;
import org.wso2.siddhi.extension.string.ConcatFunctionExtension;

import javax.xml.stream.XMLStreamException;
import java.util.concurrent.atomic.AtomicInteger;

public class ToXMLFunctionExtensionTestCase {
    private static final Logger log = Logger.getLogger(ToXMLFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testToXMLFunctionExtension() throws InterruptedException {
        log.info("ToXMLFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("str:concat", ConcatFunctionExtension.class);

        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select "
                + "map:createFromXML(\"<sensor>" +
                "<commonAttr1>19</commonAttr1>" +
                "<commonAttr2>11.45</commonAttr2>" +
                "<commonAttr3>true</commonAttr3>" +
                "<commonAttr4>ELEMENT_TEXT</commonAttr4>" +
                "<specAttributesObj>" +
                "<specAttr1>111</specAttr1>" +
                "<specAttr2>222</specAttr2>" +
                "</specAttributesObj>" +
                "</sensor>\") as hashMap insert into outputStream;" +
                "from outputStream " +
                "select map:toXML(hashMap) as xmlString " +
                "insert into outputStream2;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(
                                    AXIOMUtil.stringToOM("<commonAttr1>19</commonAttr1>"
                                            + "<commonAttr2>11.45</commonAttr2>" + "<commonAttr3>true</commonAttr3>"
                                            + "<commonAttr4>ELEMENT_TEXT</commonAttr4>"
                                            + "<specAttributesObj><specAttr1>111</specAttr1><specAttr2>222</specAttr2></specAttributesObj>"),
                                    AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100, 100l});
        inputHandler.send(new Object[]{"WSO2", 200, 200l});
        inputHandler.send(new Object[]{"XYZ", 300, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCreateFromXMLFunctionExtension2() throws InterruptedException {
        log.info("CreateFromXMLFunctionExtension TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("str:concat", ConcatFunctionExtension.class);

        String inStreamDefinition = "\ndefine stream inputStream (longAttr long, doubleAttr double, booleanAttr bool, strAttr string);";
        String query = ("@info(name = 'query1') from inputStream select " +
                "map:createFromXML(str:concat('<sensor><commonAttr1>',longAttr,'</commonAttr1><commonAttr2>'," +
                "doubleAttr,'</commonAttr2><commonAttr3>',booleanAttr,'</commonAttr3><commonAttr4>',strAttr,'</commonAttr4></sensor>')) " +
                "as hashMap insert into outputStream;" +
                "from outputStream " +
                "select map:toXML(hashMap) as xmlString " +
                "insert into outputStream2");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(
                                    AXIOMUtil.stringToOM("<commonAttr1>25</commonAttr1>"
                                            + "<commonAttr2>100.1</commonAttr2>" + "<commonAttr3>true</commonAttr3>"
                                            + "<commonAttr4>Event1</commonAttr4>"),
                                    AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(
                                    AXIOMUtil.stringToOM("<commonAttr1>35</commonAttr1>"
                                            + "<commonAttr2>100.11</commonAttr2>" + "<commonAttr3>false</commonAttr3>"
                                            + "<commonAttr4>Event2</commonAttr4>"),
                                    AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(
                                    AXIOMUtil.stringToOM("<commonAttr1>45</commonAttr1>"
                                            + "<commonAttr2>100.13456</commonAttr2>" + "<commonAttr3>true</commonAttr3>"
                                            + "<commonAttr4>Event3</commonAttr4>"),
                                    AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{25, 100.1, true, "Event1"});
        inputHandler.send(new Object[]{35, 100.11, false, "Event2"});
        inputHandler.send(new Object[]{45, 100.13456, true, "Event3"});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testToXMLFunctionExtension3() throws InterruptedException {
        log.info("ToXMLFunctionExtension TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("str:concat", ConcatFunctionExtension.class);

        String inStreamDefinition = "\ndefine stream inputStream (symbol string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream select "
                + "map:createFromXML(\"<sensor>" +
                "<commonAttr1>19</commonAttr1>" +
                "<commonAttr2>11.45</commonAttr2>" +
                "<commonAttr3>true</commonAttr3>" +
                "<commonAttr4>ELEMENT_TEXT</commonAttr4>" +
                "<specAttributesObj>" +
                "<specAttr1>111</specAttr1>" +
                "<specAttr2>222</specAttr2>" +
                "</specAttributesObj>" +
                "</sensor>\") as hashMap insert into outputStream;" +
                "from outputStream " +
                "select map:toXML(hashMap, 'sensor') as xmlString " +
                "insert into outputStream2;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(
                                    AXIOMUtil.stringToOM("<sensor>" + "<commonAttr1>19</commonAttr1>"
                                            + "<commonAttr2>11.45</commonAttr2>" + "<commonAttr3>true</commonAttr3>"
                                            + "<commonAttr4>ELEMENT_TEXT</commonAttr4>"
                                            + "<specAttributesObj><specAttr1>111</specAttr1><specAttr2>222</specAttr2></specAttributesObj>"
                                            + "</sensor>"),
                                    AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100, 100l});
        inputHandler.send(new Object[]{"WSO2", 200, 200l});
        inputHandler.send(new Object[]{"XYZ", 300, 200l});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testCreateFromXMLFunctionExtension4() throws InterruptedException {
        log.info("CreateFromXMLFunctionExtension TestCase 4");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("str:concat", ConcatFunctionExtension.class);

        String inStreamDefinition = "\ndefine stream inputStream (longAttr long, doubleAttr double, booleanAttr bool, strAttr string);";
        String query = ("@info(name = 'query1') from inputStream select " +
                "map:createFromXML(str:concat('<sensor><commonAttr1>',longAttr,'</commonAttr1><commonAttr2>'," +
                "doubleAttr,'</commonAttr2><commonAttr3>',booleanAttr,'</commonAttr3><commonAttr4>',strAttr,'</commonAttr4></sensor>')) " +
                "as hashMap insert into outputStream;" +
                "from outputStream " +
                "select map:toXML(hashMap, 'sensor') as xmlString " +
                "insert into outputStream2");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                EventPrinter.print(inEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(AXIOMUtil.stringToOM("<sensor>" +
                                    "<commonAttr1>25</commonAttr1>" +
                                    "<commonAttr2>100.1</commonAttr2>" +
                                    "<commonAttr3>true</commonAttr3>" +
                                    "<commonAttr4>Event1</commonAttr4>" +
                                    "</sensor>"), AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(AXIOMUtil.stringToOM("<sensor>" +
                                    "<commonAttr1>35</commonAttr1>" +
                                    "<commonAttr2>100.11</commonAttr2>" +
                                    "<commonAttr3>false</commonAttr3>" +
                                    "<commonAttr4>Event2</commonAttr4>" +
                                    "</sensor>"), AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        try {
                            Assert.assertEquals(true, new XMLComparator().compare(AXIOMUtil.stringToOM("<sensor>" +
                                    "<commonAttr1>45</commonAttr1>" +
                                    "<commonAttr2>100.13456</commonAttr2>" +
                                    "<commonAttr3>true</commonAttr3>" +
                                    "<commonAttr4>Event3</commonAttr4>" +
                                    "</sensor>"), AXIOMUtil.stringToOM((String) event.getData(0))));
                        } catch (XMLComparisonException e) {
                            log.error("Error comparing two XML elements:" + e.getMessage(), e);
                        } catch (XMLStreamException e) {
                            log.error("Error parsing XML:" + e.getMessage(), e);
                        }
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{25, 100.1, true, "Event1"});
        inputHandler.send(new Object[]{35, 100.11, false, "Event2"});
        inputHandler.send(new Object[]{45, 100.13456, true, "Event3"});
        SiddhiTestHelper.waitForEvents(100, 3, count, 60000);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}

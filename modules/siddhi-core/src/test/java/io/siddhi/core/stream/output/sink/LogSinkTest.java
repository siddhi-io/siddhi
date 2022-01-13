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

package io.siddhi.core.stream.output.sink;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.UnitTestAppender;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class LogSinkTest {
    @Test
    public void testWithAllOptions() throws Exception {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "@App:name(\"HelloWorldApp\")\n" +
                "define stream CargoStream (weight int);";
        String outputStream = "@sink(type='log', prefix='My Log',  priority='info')\n" +
                "define stream OutputStream(weight int, totalWeight long);";

        String query = (
                "@info(name='HelloWorldQuery') " +
                        "from CargoStream " +
                        "select weight, sum(weight) as totalWeight " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + outputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");

        try {
            inputHandler.send(new Object[]{2});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("My Log"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[2, 2]"));
            inputHandler.send(new Object[]{3});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("My Log"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[3, 5]"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing with all options", e);
        } finally {
            logger.removeAppender(appender);
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testWithDefaultPrefix() throws Exception {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "@App:name(\"HelloWorldApp\")\n" +
                "define stream CargoStream (weight int);";
        String outputStream = "@sink(type='log',  priority='info')\n" +
                "define stream OutputStream(weight int, totalWeight long);";

        String query = (
                "@info(name='HelloWorldQuery') " +
                        "from CargoStream " +
                        "select weight, sum(weight) as totalWeight " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + outputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");

        try {
            inputHandler.send(new Object[]{2});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("HelloWorldApp : OutputStream"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[2, 2]"));
            inputHandler.send(new Object[]{3});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("HelloWorldApp : OutputStream"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[3, 5]"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing with default prefix", e);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testWithDefaultPriority() throws Exception {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "@App:name(\"HelloWorldApp\")\n" +
                "define stream CargoStream (weight int);";
        String outputStream = "@sink(type='log', prefix='My Log')\n" +
                "define stream OutputStream(weight int, totalWeight long);";

        String query = (
                "@info(name='HelloWorldQuery') " +
                        "from CargoStream " +
                        "select weight, sum(weight) as totalWeight " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + outputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");

        try {
            inputHandler.send(new Object[]{2});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("My Log"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[2, 2]"));
            inputHandler.send(new Object[]{3});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("My Log"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[3, 5]"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing with default priority", e);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testWithDefaultOptions() throws Exception {
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String inputStream = "@App:name(\"HelloWorldApp\")\n" +
                "define stream CargoStream (weight int);";
        String outputStream = "@sink(type='log')\n" +
                "define stream OutputStream(weight int, totalWeight long);";

        String query = (
                "@info(name='HelloWorldQuery') " +
                        "from CargoStream " +
                        "select weight, sum(weight) as totalWeight " +
                        "insert into OutputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inputStream + outputStream + query);

        siddhiAppRuntime.start();
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("CargoStream");

        try {
            inputHandler.send(new Object[]{2});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("HelloWorldApp : OutputStream"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[2, 2]"));
            inputHandler.send(new Object[]{3});
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("HelloWorldApp : OutputStream"));
            AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                    get("UnitTestAppender")).getMessages().contains("data=[3, 5]"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception occurred when testing with default options", e);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

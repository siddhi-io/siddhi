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
package io.siddhi.core.config;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.extension.util.StringConcatAggregatorExecutorString;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.FileReader;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.config.YAMLConfigManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class YAMLConfigManagerTestCase {
    private static final Logger log = LogManager.getLogger(YAMLConfigManagerTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void yamlConfigManagerTest1() {
        log.info("YAMLConfigManagerTest1");

        String baseDir = Paths.get(".").toString();
        Path filePath = Paths.get(baseDir, "src", "test", "resources", "systemProperties.yaml");

        String fileContent = FileReader.readYAMLConfigFile(filePath);

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(fileContent);

        Map<String, String> testConfigs = yamlConfigManager.extractSystemConfigs("test");
        Assert.assertEquals(testConfigs.size(), 0);

        Map<String, String> source1Ref = yamlConfigManager.extractSystemConfigs("ref1");
        Assert.assertEquals(source1Ref.size(), 3);
        Assert.assertEquals(source1Ref.get("type"), "testStoreContainingInMemoryTable");
        Assert.assertEquals(source1Ref.get("property1"), "value1");
        Assert.assertEquals(source1Ref.get("property2"), "value2");
        Assert.assertNull(source1Ref.get("property3"));

        String partitionById = yamlConfigManager.extractProperty("partitionById");
        Assert.assertEquals(partitionById, "TRUE");

        String shardID1 = yamlConfigManager.extractProperty("shardId1");
        Assert.assertNull(shardID1);

        ConfigReader testConfigReader = yamlConfigManager.generateConfigReader("test", "test");
        Assert.assertEquals(testConfigReader.getAllConfigs().size(), 0);

        ConfigReader configReader = yamlConfigManager.generateConfigReader("store", "rdbms");
        Assert.assertEquals(configReader.getAllConfigs().size(), 5);
        Assert.assertEquals(configReader.readConfig("mysql.batchEnable", "test"), "true");
        Assert.assertEquals(configReader.readConfig("test", "test"), "test");

    }

    @Test
    public void yamlConfigManagerTest2() throws InterruptedException {
        log.info("YAMLConfigManagerTest2 - References test");

        String baseDir = Paths.get(".").toString();
        Path path = Paths.get(baseDir, "src", "test", "resources", "systemProperties.yaml");

        String fileContent = FileReader.readYAMLConfigFile(path);

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(fileContent);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(ref='ref1')\n" +
                "define table StockTable (symbol string, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);

        siddhiAppRuntime.shutdown();

    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void yamlConfigManagerTest3() {
        log.info("YAMLConfigManagerTest2 - Undefined References test");

        String baseDir = Paths.get(".").toString();
        Path path = Paths.get(baseDir, "src", "test", "resources", "systemProperties.yaml");

        String fileContent = FileReader.readYAMLConfigFile(path);

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(fileContent);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(ref='ref2')\n" +
                "define table StockTable (symbol string, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        siddhiAppRuntime.start();
    }

    @Test
    public void yamlConfigManagerTest4() throws InterruptedException {
        log.info("yamlConfigManagerTest4");

        String baseDir = Paths.get(".").toString();
        Path path = Paths.get(baseDir, "src", "test", "resources", "systemProperties.yaml");

        String fileContent = FileReader.readYAMLConfigFile(path);

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(fileContent);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("email:getAllNew", StringConcatAggregatorExecutorString.class);
        siddhiManager.setConfigManager(yamlConfigManager);


        String cseEventStream = "" +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);";
        String query = ("" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select price , email:getAllNew(symbol,'') as toConcat " +
                "group by volume " +
                "insert into mailOutput;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);


        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count = count + inEvents.length;
                if (count == 3) {
                    AssertJUnit.assertEquals("WSO2ABC-abc", inEvents[inEvents.length - 1].getData(1));
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"ABC", 60.5f, 200L});
        Thread.sleep(100);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

}

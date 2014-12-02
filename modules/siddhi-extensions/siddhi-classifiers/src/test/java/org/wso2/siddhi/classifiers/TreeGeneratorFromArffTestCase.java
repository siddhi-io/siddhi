/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.classifiers.utils.MOADataSetParser;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;

public class TreeGeneratorFromArffTestCase {
    static final Logger log = Logger.getLogger(TreeGeneratorFromArffTestCase.class);

    private int inEventCount = 0;
    private int removeEventCount = 0;
    private boolean eventArrived;
    SiddhiConfiguration configuration;
    SiddhiManager siddhiManager;

    @Before
    public void initialize() {
        eventArrived = false;
        configuration = new SiddhiConfiguration();
        configuration.setAsyncProcessing(false);
        siddhiManager = new SiddhiManager(configuration);
    }

    @Test
    public void weatherDataTest() throws InterruptedException {
        log.info("Hoeffding Tree test is Running...");
        String streamName = "weather";
        String trainingFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/weather.nominal-train.arff";
        String testFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/weather.nominal-test.arff";
        String airline = MOADataSetParser.getStreamDefinition(streamName, testFileName);
        siddhiManager.defineStream(airline);
        StreamDefinition streamDefinition = siddhiManager.getStreamDefinition(streamName);
        siddhiManager.addQuery("from " + streamName + "#window.classifyHt(8,1)" +
                "select " + streamDefinition.getAttributeList().get(((ArrayList) streamDefinition.getAttributeList()).size()-1).getName() + " " +
                "insert into Results for all-events ;");

        InputHandler loginSucceedEvents = siddhiManager.getInputHandler(streamName);
        MOADataSetParser.sendAllEvents(loginSucceedEvents,trainingFileName); // sending training data-set
        MOADataSetParser.sendAllEvents(loginSucceedEvents,testFileName); // sending training data-set
        MOADataSetParser.createClassValueFile(testFileName,streamDefinition);
        MOADataSetParser.evaluate("/tmp/weather-original.txt","/tmp/weather-evaluation.txt");
        Thread.sleep(1000);
    }

    @Test
    public void contactLensesDataTest() throws InterruptedException {
        log.info("Hoeffding Tree test is Running...");
        String streamName = "contactlenses";
        String trainingFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/contact-lenses-train.arff";
        String testFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/contact-lenses-test.arff";
        String airline = MOADataSetParser.getStreamDefinition(streamName, testFileName);
        siddhiManager.defineStream(airline);
        StreamDefinition streamDefinition = siddhiManager.getStreamDefinition(streamName);
        siddhiManager.addQuery("from " + streamName + "#window.classifyHt(14,1)" +
                "select " + streamDefinition.getAttributeList().get(((ArrayList) streamDefinition.getAttributeList()).size()-1).getName() + " " +
                "insert into Results for all-events ;");

        InputHandler loginSucceedEvents = siddhiManager.getInputHandler(streamName);
        MOADataSetParser.sendAllEvents(loginSucceedEvents,trainingFileName); // sending training data-set
        MOADataSetParser.sendAllEvents(loginSucceedEvents,testFileName); // sending training data-set
        MOADataSetParser.createClassValueFile(testFileName, streamDefinition);
        MOADataSetParser.evaluate("/tmp/contactlenses-original.txt", "/tmp/contactlenses-evaluation.txt");
        Thread.sleep(1000);
    }

    @Test
    public void breastCancerDataTest() throws InterruptedException {
        log.info("Hoeffding Tree test is Running...");
        String streamName = "breastCancer";
        String trainingFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/breast-cancer-train.arff";
        String testFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/breast-cancer-cv.arff";
        String airline = MOADataSetParser.getStreamDefinition(streamName, testFileName);
        siddhiManager.defineStream(airline);
        StreamDefinition streamDefinition = siddhiManager.getStreamDefinition(streamName);
        siddhiManager.addQuery("from " + streamName + "#window.classifyHt(171,1)" +
                "select " + streamDefinition.getAttributeList().get(((ArrayList) streamDefinition.getAttributeList()).size()-1).getName() + " " +
                "insert into Results for all-events ;");

        InputHandler loginSucceedEvents = siddhiManager.getInputHandler(streamName);
        MOADataSetParser.sendAllEvents(loginSucceedEvents,trainingFileName); // sending training data-set
        MOADataSetParser.sendAllEvents(loginSucceedEvents,testFileName); // sending training data-set
        MOADataSetParser.createClassValueFile(testFileName,streamDefinition);
        MOADataSetParser.evaluate("/tmp/breastCancer-original.txt","/tmp/breastCancer-evaluation.txt");
        Thread.sleep(1000);
    }


    @Test
    public void eslDataTest() throws InterruptedException {
        log.info("Hoeffding Tree test is Running...");
        String streamName = "esl";
        String trainingFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/esl-train.arff";
        String testFileName = "/Users/lginnali/masters/independent-study-01/siddhi/modules/siddhi-extensions/siddhi-classifiers/src/test/resources/esl-test.arff";
        String airline = MOADataSetParser.getStreamDefinition(streamName, testFileName);
        siddhiManager.defineStream(airline);
        StreamDefinition streamDefinition = siddhiManager.getStreamDefinition(streamName);
        siddhiManager.addQuery("from " + streamName + "#window.classifyHt(600,1)" +
                "select " + streamDefinition.getAttributeList().get(((ArrayList) streamDefinition.getAttributeList()).size()-1).getName() + " " +
                "insert into Results for all-events ;");

        InputHandler loginSucceedEvents = siddhiManager.getInputHandler(streamName);
        MOADataSetParser.sendAllEvents(loginSucceedEvents,trainingFileName); // sending training data-set
        MOADataSetParser.sendAllEvents(loginSucceedEvents,testFileName); // sending training data-set
        MOADataSetParser.createClassValueFile(testFileName,streamDefinition);
        MOADataSetParser.evaluate("/tmp/esl-original.txt","/tmp/esl-evaluation.txt");
        Thread.sleep(1000);
    }





}

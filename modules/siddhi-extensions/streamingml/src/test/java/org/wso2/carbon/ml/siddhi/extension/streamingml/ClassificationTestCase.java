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

package org.wso2.carbon.ml.siddhi.extension.streamingml;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

public class ClassificationTestCase {

    private static final Logger logger = Logger.getLogger(ClassificationTestCase.class);
    private volatile int count;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void testClassificationStreamProcessorExtension() throws InterruptedException {
        logger.info("StreamingClasificationStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = " define stream inputStream (attribute_0 double, " +
                "attribute_1 double, attribute_2 double,attribute_3 double, attribute_4 string );";
        String query = ("@info(name = 'query1') from inputStream#streamingml:" +
                "streamingClassificationSamoa(5,3,0,'',10000,attribute_0, attribute_1 , " +
                "attribute_2 ,attribute_3,attribute_4) select att_0 as attribute_0, " +
                "att_1 as attribute_1,att_2 as attribute_2,att_3 as attribute_3," +
                " prediction as prediction insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {

            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                count++;
                if (count == 1) {
                    Assert.assertEquals("setosa", inEvents[0].getData()[4]);
                }
                if (count == 2) {
                    Assert.assertEquals("versicolor", inEvents[0].getData()[4]);
                }
                if (count == 3) {
                    Assert.assertEquals("virginica", inEvents[0].getData()[4]);
                }
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        Scanner scn =null;
        try {
            File f = new File("src/test/resources/iris.csv");
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            scn = new Scanner(br);

            InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
            executionPlanRuntime.start();
            while (true) {
                if (scn.hasNext()) {
                    String eventStr = scn.nextLine();
                    String[] event = eventStr.split(",");
                    inputHandler.send(new Object[]{Double.valueOf(event[0]),Double.valueOf(event[1])
                            , Double.valueOf(event[2]), Double.valueOf(event[3]),  event[4]});
                } else {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            Thread.sleep(1100);
            Assert.assertEquals(3, count);
            executionPlanRuntime.shutdown();
        } catch (Exception e) {
            logger.info(e.toString());
        }finally {
            scn.close();
        }
    }

}

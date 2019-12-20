/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.performance;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

public class SimpleWindowSingleQueryPerformance {

    public static void main(String[] args) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume int, timestamp long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.length(10) " +
                "select symbol, sum(price) as total, avg(volume) as avgVolume, timestamp " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            public int eventCount = 0;
            public int timeSpent = 0;
            long startTime = System.currentTimeMillis();

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    eventCount++;
                    timeSpent += (System.currentTimeMillis() - (Long) event.getData(3));
                    if (eventCount % 10000000 == 0) {
                        System.out.println("Throughput : " + (eventCount * 1000) / ((System.currentTimeMillis()) -
                                startTime));
                        System.out.println("Time spent :  " + (timeSpent * 1.0 / eventCount));
                        startTime = System.currentTimeMillis();
                        eventCount = 0;
                        timeSpent = 0;
                    }
                }
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        while (true) {
            inputHandler.send(new Object[]{"WSO2", 55.6f, 100, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"WSO2", 100f, 80, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"WSO2", 55.6f, 100, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"WSO2", 100f, 80, System.currentTimeMillis()});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100, System.currentTimeMillis()});
        }

    }
}

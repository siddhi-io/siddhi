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

import java.util.concurrent.atomic.AtomicInteger;

public class SimplePartitionedDoubleFilterQueryPerformance {

    public static void main(String[] args) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "define stream cseEventStream (symbol string, price float, volume long, timestamp long);" +
                "" +
                "partition with (symbol of cseEventStream) " +
                "begin " +
                "   @info(name = 'query1') " +
                "   from cseEventStream[700 > price] " +
                "   select * " +
                "   insert into #innerOutputStream ; " +
                "" +
                "   @info(name = 'query2') " +
                "   from #innerOutputStream[700 > price] " +
                "   select * " +
                "   insert into outputStream ;" +
                "end;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            public AtomicInteger eventCount = new AtomicInteger();
            public int timeSpent = 0;
            long startTime = System.currentTimeMillis();

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    int count = eventCount.incrementAndGet();
                    timeSpent += (System.currentTimeMillis() - (Long) event.getData(3));
                    if (count % 10000000 == 0) {
                        System.out.println("Throughput : " + (count * 1000) / ((System.currentTimeMillis()) -
                                startTime));
                        System.out.println("Time spent :  " + (timeSpent * 1.0 / count));
                        startTime = System.currentTimeMillis();
                        eventCount.set(0);
                        timeSpent = 0;
                    }
                }
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();

        for (int i = 0; i <= 10; i++) {
            EventPublisher eventPublisher = new EventPublisher(inputHandler);
            eventPublisher.start();
        }
        //siddhiAppRuntime.shutdown();
    }


    static class EventPublisher extends Thread {

        InputHandler inputHandler;

        EventPublisher(InputHandler inputHandler) {
            this.inputHandler = inputHandler;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    inputHandler.send(new Object[]{"1", 55.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"2", 75.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"3", 100f, 80, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"4", 75.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"5", 55.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"6", 75.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"7", 100f, 80, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"8", 75.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"9", 75.6f, 100, System.currentTimeMillis()});
                    inputHandler.send(new Object[]{"10", 75.6f, 100, System.currentTimeMillis()});
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

        }
    }
}

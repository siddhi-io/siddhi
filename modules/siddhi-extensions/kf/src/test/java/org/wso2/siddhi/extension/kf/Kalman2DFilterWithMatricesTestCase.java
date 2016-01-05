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

package org.wso2.siddhi.extension.kf;

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


public class Kalman2DFilterWithMatricesTestCase {
    static final Logger log = Logger.getLogger(Kalman2DFilterWithMatricesTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testKalman2DFilterWithMatricesPOC() throws InterruptedException {
        log.info("kalman2DFilterWithMatrices TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(
                "@config(async = 'true') define stream cleanedStream (latitude double, longitude double, " +
                    "changingRate double, measurementNoiseSD double, timestamp long, recordLocator string, level int); "
                +"@info(name = 'query1') from cleanedStream#kf:kalman2DFilter(latitude, " +
                    "longitude, changingRate, measurementNoiseSD, timestamp, recordLocator, level) " +
                    " select * insert into dataOut;");

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count++;
                    if (count == 1) {
                        Assert.assertEquals(-74.1784439700006d, event.getData(7));
                        Assert.assertEquals(40.6958810299994d, event.getData(8));
                        eventArrived = true;
                    }
                    if (count == 2) {
                        Assert.assertEquals(-74.17657538193608d, event.getData(7));
                        Assert.assertEquals(40.69711415696983d, event.getData(8));
                        eventArrived = true;
                    }
                    if (count == 3) {
                        Assert.assertEquals(-74.17487924016262d, event.getData(7));
                        Assert.assertEquals(40.69632380976617d, event.getData(8));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cleanedStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{-74.178444,40.695881, 0.003, 0.01d, 1445234861l, "id1", 1});
        Thread.sleep(500);
        inputHandler.send(new Object[]{-74.177872,40.695702, 0.003, 0.01d, 1445234864l, "id1", 1});
        Thread.sleep(500);
        inputHandler.send(new Object[]{-74.175703,40.694852999999995, 0.003, 0.01d, 1445234867l, "id1", 1});
        Thread.sleep(100);

        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);

        executionPlanRuntime.shutdown();
    }
}

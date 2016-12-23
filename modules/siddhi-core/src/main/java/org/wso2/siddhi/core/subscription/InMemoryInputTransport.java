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

package org.wso2.siddhi.core.subscription;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InMemoryInputTransport extends InputTransport {

    private InputCallback inputCallback;
    private ScheduledExecutorService executorService;
    private DataGenerator dataGenerator = new DataGenerator();

    @Override
    public void init(Map<String, String> transportOptions, InputCallback inputCallback) {
        System.out.println("Transport options: " + transportOptions);
        this.inputCallback = inputCallback;
        this.executorService = Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setNameFormat("Siddhi-inmemoryinputtransport-scheduler-thread-%d").build());
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        System.out.println("Connecting");
        this.executorService.scheduleAtFixedRate(dataGenerator, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void disconnect() {
        System.out.println("Disconnecting");
        this.executorService.shutdown();
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean isEventDuplicatedInCluster() {
        return false;
    }

    @Override
    public boolean isPolling() {
        return false;
    }

    private class DataGenerator implements Runnable {

        @Override
        public void run() {
//            inputCallback.onEvent(new Object[]{"WSO2", 56.75f, 5});
            inputCallback.onEvent("{'symbol': 'WSO2', 'price': 56.75, 'volume': 5, 'country': 'Sri Lanka'}");
//            inputCallback.onEvent("WSO2,56.75,5,Sri Lanka");
//            inputCallback.onEvent("symbol=WSO2, price=56.75, volume=5, country=Sri Lanka");
        }
    }
}

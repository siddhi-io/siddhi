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
package org.wso2.siddhi.core.debugger;

import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.List;

/**
 * Created on 7/21/16.
 */
public class AsyncThread implements Runnable {
    InputHandler inputHandler;
    List<Object[]> eventsToSend;

    public AsyncThread(InputHandler inputHandler, List<Object[]> eventsToSend) {
        this.inputHandler = inputHandler;
        this.eventsToSend = eventsToSend;
    }

    @Override
    public void run() {

        try {
            for (Object[] events:eventsToSend) {
                inputHandler.send(events);
                Thread.sleep(1000);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

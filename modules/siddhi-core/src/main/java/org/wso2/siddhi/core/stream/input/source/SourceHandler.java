/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.stream.input.source;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

/**
 * SourceHandler is an optional implementable class that wraps {@link org.wso2.siddhi.core.stream.input.InputHandler}.
 * It will do optional processing to the events before sending the events to the input handler
 */
public abstract class SourceHandler implements InputEventHandlerCallback, Snapshotable {

    private String elementId;
    private InputHandler inputHandler;

    final void initSourceHandler(String siddhiAppName, SourceSyncCallback sourceSyncCallback, String elementId,
                                 StreamDefinition streamDefinition) {
        this.elementId = elementId;
        init(siddhiAppName, sourceSyncCallback, elementId, streamDefinition);
    }

    public abstract void init(String siddhiAppName, SourceSyncCallback sourceSyncCallback, String elementId,
                              StreamDefinition streamDefinition);

    @Override
    public void sendEvent(Event event, String[] transportSyncProperties) throws InterruptedException {
        sendEvent(event, transportSyncProperties, inputHandler);
    }

    @Override
    public void sendEvents(Event[] events, String[] transportSyncProperties) throws InterruptedException {
        sendEvent(events, transportSyncProperties, inputHandler);
    }

    public abstract void sendEvent(Event event, String[] transportSyncProperties, InputHandler inputHandler)
            throws InterruptedException;

    public abstract void sendEvent(Event[] events, String[] transportSyncProperties, InputHandler inputHandler)
            throws InterruptedException;

    public String getElementId() {
        return elementId;
    }

    public void setInputHandler(InputHandler inputHandler) {
        this.inputHandler = inputHandler;
    }

    public InputHandler getInputHandler() {
        return inputHandler;
    }

    @Override
    public void clean() {
        //ignore
    }
}

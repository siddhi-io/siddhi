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

package io.siddhi.core.stream.input.source;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * SourceHandler is an optional implementable class that wraps {@link InputHandler}.
 * It will do optional processing to the events before sending the events to the input handler
 *
 * @param <S> current state for the Source Holder
 */
public abstract class SourceHandler<S extends State> implements InputEventHandlerCallback {

    private InputHandler inputHandler;
    private StateHolder<S> stateHolder;
    private String id;

    final void initSourceHandler(String siddhiAppName, SourceSyncCallback sourceSyncCallback,
                                 StreamDefinition streamDefinition, SiddhiAppContext siddhiAppContext) {
        StateFactory<S> stateFactory = init(siddhiAppName, sourceSyncCallback, streamDefinition, siddhiAppContext);
        id = siddhiAppName + "-" + streamDefinition.getId() + "-" + this.getClass().getName();
        stateHolder = siddhiAppContext.generateStateHolder(
                streamDefinition.getId() + "-" + this.getClass().getName(),
                stateFactory);
    }

    public abstract StateFactory init(String siddhiAppName, SourceSyncCallback sourceSyncCallback,
                                      StreamDefinition streamDefinition, SiddhiAppContext siddhiAppContext);

    @Override
    public void sendEvent(Event event, String[] transportSyncProperties) throws InterruptedException {
        S state = stateHolder.getState();
        try {
            sendEvent(event, transportSyncProperties, state, inputHandler);
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public void sendEvents(Event[] events, String[] transportSyncProperties) throws InterruptedException {
        S state = stateHolder.getState();
        try {
            sendEvent(events, transportSyncProperties, state, inputHandler);
        } finally {
            stateHolder.returnState(state);
        }
    }

    public abstract void sendEvent(Event event, String[] transportSyncProperties, S state, InputHandler inputHandler)
            throws InterruptedException;

    public abstract void sendEvent(Event[] events, String[] transportSyncProperties, S state, InputHandler inputHandler)
            throws InterruptedException;

    public InputHandler getInputHandler() {
        return inputHandler;
    }

    public void setInputHandler(InputHandler inputHandler) {
        this.inputHandler = inputHandler;
    }

    public String getId() {
        return id;
    }
}

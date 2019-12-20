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

package io.siddhi.core.stream.output.sink;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * SinkHandler is an optional interface before {@link SinkMapper}.
 * It will do optional processing to the events before sending the events to the desired mapper
 *
 * @param <S> current state for the Sink Holder
 */
public abstract class SinkHandler<S extends State> {

    private SinkHandlerCallback sinkHandlerCallback;
    private StateHolder<S> stateHolder;
    private String id;

    final void initSinkHandler(String siddhiAppName, StreamDefinition streamDefinition,
                               SinkHandlerCallback sinkHandlerCallback,
                               SiddhiAppContext siddhiAppContext) {
        this.sinkHandlerCallback = sinkHandlerCallback;
        StateFactory<S> stateFactory = init(streamDefinition, sinkHandlerCallback);
        id = siddhiAppName + "-" + streamDefinition.getId() + "-" + this.getClass().getName();
        stateHolder = siddhiAppContext.generateStateHolder(
                streamDefinition.getId() + "-" + this.getClass().getName(),
                stateFactory);
    }

    public abstract StateFactory<S> init(StreamDefinition streamDefinition,
                                         SinkHandlerCallback sinkHandlerCallback);

    public void handle(Event event) {
        S state = stateHolder.getState();
        try {
            handle(event, sinkHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    public void handle(Event[] events) {
        if (stateHolder != null) {
            S state = stateHolder.getState();
            try {
                handle(events, sinkHandlerCallback, state);
            } finally {
                stateHolder.returnState(state);
            }
        } else {
            handle(events, sinkHandlerCallback, null);
        }
    }

    public abstract void handle(Event event, SinkHandlerCallback sinkHandlerCallback, S state);

    public abstract void handle(Event[] events, SinkHandlerCallback sinkHandlerCallback, S state);

    public String getId() {
        return id;
    }
}

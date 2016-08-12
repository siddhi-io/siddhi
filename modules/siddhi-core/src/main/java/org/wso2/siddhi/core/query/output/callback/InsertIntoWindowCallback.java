/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.core.query.output.callback;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.window.EventWindow;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

/**
 * This callback is an adapter between {@link org.wso2.siddhi.core.stream.StreamJunction} and {@link EventWindow}.
 * It receives {@link ComplexEventChunk}s and insert them into {@link EventWindow}.
 */
public class InsertIntoWindowCallback extends OutputCallback {
    /**
     * EventWindow to which the events have to be inserted.
     */
    private final EventWindow eventWindow;

    /**
     * StreamDefinition of the window.
     */
    private final StreamDefinition outputStreamDefinition;

    public InsertIntoWindowCallback(EventWindow eventWindow, StreamDefinition outputStreamDefinition) {
        this.eventWindow = eventWindow;
        this.outputStreamDefinition = outputStreamDefinition;
    }

    /**
     * Add the event into the {@link EventWindow}
     *
     * @param complexEventChunk the event to add
     */
    @Override
    public void send(ComplexEventChunk complexEventChunk) {
        // If events are inserted directly from another window, expired events can arrive
        complexEventChunk.reset();
        while (complexEventChunk.hasNext()) {
            ComplexEvent complexEvent = complexEventChunk.next();
            if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                complexEvent.setType(ComplexEvent.Type.CURRENT);
            }
        }
        eventWindow.add(complexEventChunk);
    }

    /**
     * Return the StreamDefinition of the corresponding window.
     *
     * @return stream definition of the target window
     */
    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    /**
     * Return the {@link EventWindow} associated with this callback.
     *
     * @return the EventWindow
     */
    public EventWindow getEventWindow() {
        return eventWindow;
    }
}

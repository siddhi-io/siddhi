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
package org.wso2.siddhi.core.stream.input;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;

/**
 * InputHandler is the {@link Event} entry point to Siddhi. Users can create an Input Handler and then use that to
 * directly inject events inside Siddhi.
 */
public class InputHandler {

    private static final Logger log = Logger.getLogger(InputHandler.class);

    protected String streamId;
    protected int streamIndex;
    protected InputProcessor inputProcessor;
    protected SiddhiAppContext siddhiAppContext;
    protected InputProcessor pausedInputPublisher;

    public InputHandler(String streamId, int streamIndex, InputProcessor inputProcessor,
                        SiddhiAppContext siddhiAppContext) {
        this.streamId = streamId;
        this.streamIndex = streamIndex;
        this.inputProcessor = inputProcessor;
        this.siddhiAppContext = siddhiAppContext;
        this.pausedInputPublisher = this.inputProcessor;
    }

    public String getStreamId() {
        return streamId;
    }

    public void send(Object[] data) throws InterruptedException {
        if (inputProcessor != null) {
            inputProcessor.send(System.currentTimeMillis(), data, streamIndex);
        }
    }

    public void send(long timestamp, Object[] data) throws InterruptedException {
        // Set timestamp to system if Siddhi is in playback mode
        if (siddhiAppContext.isPlayback()) {
            this.siddhiAppContext.getTimestampGenerator().setCurrentTimestamp(timestamp);
        }
        if (inputProcessor != null) {
            inputProcessor.send(timestamp, data, streamIndex);
        }
    }

    public void send(Event event) throws InterruptedException {
        // Set timestamp to system if Siddhi is in playback mode
        if (siddhiAppContext.isPlayback()) {
            this.siddhiAppContext.getTimestampGenerator().setCurrentTimestamp(event.getTimestamp());
        }
        if (inputProcessor != null) {
            inputProcessor.send(event, streamIndex);
        }
    }

    public void send(Event[] events) throws InterruptedException {
        // Set timestamp to system if Siddhi is in playback mode
        if (siddhiAppContext.isPlayback() && events.length != 0) {
            this.siddhiAppContext.getTimestampGenerator().setCurrentTimestamp(events[events.length - 1].getTimestamp());
        }
        if (inputProcessor != null) {
            inputProcessor.send(events, streamIndex);
        }
    }

    void disconnect() {
        this.inputProcessor = null;
    }

    void resume() {
        this.inputProcessor = pausedInputPublisher;
    }
}

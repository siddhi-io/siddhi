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

package org.wso2.siddhi.core.publisher;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import org.wso2.siddhi.core.query.output.callback.OutputCallback;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

public class PassThroughOutputMapper implements OutputMapper {
    private StreamDefinition outputStreamDefinition;
    private OutputCallback outputCallback;
    private StreamEventPool streamEventPool;
    private StreamEventConverter streamEventConverter;

    @Override
    public StreamDefinition getOutputStreamDefinition() {
        return this.outputStreamDefinition;
    }

    @Override
    public void inferOutputStreamDefinition(StreamDefinition outputStreamDefinition) {
        this.outputStreamDefinition = outputStreamDefinition;
    }

    @Override
    public void init(OutputCallback outputCallback, MetaStreamEvent metaStreamEvent) {
        this.outputCallback = outputCallback;
        this.outputStreamDefinition = metaStreamEvent.getOutputStreamDefinition();
        this.streamEventConverter = new ZeroStreamEventConverter();
        this.streamEventPool = new StreamEventPool(metaStreamEvent, 5);
    }

    @Override
    public void onEvent(Object eventObject) {
        Event event;
        if (eventObject == null) {
            throw new IllegalArgumentException("Event object must be either Event or Object[] but found null");
        } else if (eventObject instanceof Event) {
            event = (Event) eventObject;
        } else if (eventObject instanceof Object[]) {
            Object[] data = (Object[]) eventObject;
            event = new Event(data.length);
            System.arraycopy(data, 0, event.getData(), 0, data.length);
        } else {
            throw new IllegalArgumentException("Event object must be either Event or Object[] but found " + eventObject.getClass().getCanonicalName());
        }
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertEvent(event, borrowedEvent);

        outputCallback.send(new ComplexEventChunk<StreamEvent>(borrowedEvent, borrowedEvent, true));
    }
}
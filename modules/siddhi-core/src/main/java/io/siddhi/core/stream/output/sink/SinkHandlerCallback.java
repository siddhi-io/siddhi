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

import io.siddhi.core.event.Event;

/**
 * Sink Handler Callback is used to send events to the {@link SinkMapper} after being handled by {@link SinkHandler}.
 */
public class SinkHandlerCallback {

    private SinkMapper sinkMapper;

    public SinkHandlerCallback(SinkMapper sinkMapper) {
        this.sinkMapper = sinkMapper;
    }

    public void mapAndSend(Event event) {
        sinkMapper.mapAndSend(event);
    }

    public void mapAndSend(Event[] events) {
        sinkMapper.mapAndSend(events);
    }
}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.query.output.callback;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.debugger.SiddhiDebugger;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * This callback is an adapter between {@link StreamJunction} and {@link Window}.
 * It receives {@link ComplexEventChunk}s from partitioned queries and insert them into {@link Window}.
 */
public class InsertIntoWindowEndPartitionCallback extends InsertIntoWindowCallback {

    public InsertIntoWindowEndPartitionCallback(Window window, StreamDefinition outputStreamDefinition,
                                                String queryName) {
        super(window, outputStreamDefinition, queryName);
    }

    /**
     * Add the event into the {@link Window}
     *
     * @param complexEventChunk the event to add
     * @param noOfEvents        number of events
     */
    @Override
    public void send(ComplexEventChunk complexEventChunk, int noOfEvents) {
        if (getSiddhiDebugger() != null) {
            getSiddhiDebugger()
                    .checkBreakPoint(getQueryName(), SiddhiDebugger.QueryTerminal.OUT, complexEventChunk.getFirst());
        }
        // If events are inserted directly from another window, expired events can arrive
        complexEventChunk.reset();
        if (complexEventChunk.getFirst() != null) {
            String flowId = SiddhiAppContext.getCurrentFlowId();
            SiddhiAppContext.stopPartitionFlow();
            try {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent complexEvent = complexEventChunk.next();
                    if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                        complexEvent.setType(ComplexEvent.Type.CURRENT);
                    }
                }
                window.add(complexEventChunk);
            } finally {
                SiddhiAppContext.startPartitionFlow(flowId);
            }
        }
    }
}

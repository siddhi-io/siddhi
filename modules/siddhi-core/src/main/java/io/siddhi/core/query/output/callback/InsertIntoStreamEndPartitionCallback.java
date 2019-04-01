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
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * Implementation of {@link OutputCallback} to receive processed Siddhi events from partitioned
 * Siddhi queries and put them into {@link StreamJunction}.
 */
public class InsertIntoStreamEndPartitionCallback extends InsertIntoStreamCallback {

    public InsertIntoStreamEndPartitionCallback(StreamDefinition outputStreamDefinition, String queryName) {
        super(outputStreamDefinition, queryName);
    }

    @Override
    public void send(ComplexEventChunk complexEventChunk, int noOfEvents) {
        if (getSiddhiDebugger() != null) {
            getSiddhiDebugger()
                    .checkBreakPoint(getQueryName(), SiddhiDebugger.QueryTerminal.OUT, complexEventChunk.getFirst());
        }
        complexEventChunk.reset();
        if (complexEventChunk.getFirst() != null) {
            String flowId = SiddhiAppContext.getPartitionFlowId();
            SiddhiAppContext.stopPartitionFlow();
            try {
                while (complexEventChunk.hasNext()) {
                    ComplexEvent complexEvent = complexEventChunk.next();
                    if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                        complexEvent.setType(ComplexEvent.Type.CURRENT);
                    }
                }
                publisher.send(complexEventChunk.getFirst());
            } finally {
                SiddhiAppContext.startPartitionFlow(flowId);
            }
        }
    }

}

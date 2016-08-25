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
package org.wso2.siddhi.core.query.output.callback;

import org.wso2.siddhi.core.debugger.SiddhiBreakPoint;
import org.wso2.siddhi.core.debugger.SiddhiDebugger;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

public class InsertIntoStreamCallback extends OutputCallback {
    private StreamDefinition outputStreamDefinition;
    private StreamJunction.Publisher publisher;
    private SiddhiBreakPoint siddhiBreakPoint;
    private String queryName;

    public InsertIntoStreamCallback(StreamDefinition outputStreamDefinition, String queryName) {
        this.outputStreamDefinition = outputStreamDefinition;
        this.queryName=queryName;
    }

    public void init(StreamJunction outputStreamJunction) {
        this.publisher = outputStreamJunction.constructPublisher();
    }
    public void setSiddhiBreakPoint(SiddhiBreakPoint siddhiBreakPoint) {
        this.siddhiBreakPoint = siddhiBreakPoint;
    }

    @Override
    public void send(ComplexEventChunk complexEventChunk) {
        if (siddhiBreakPoint != null) {
            siddhiBreakPoint.checkBreakPoint(queryName , SiddhiDebugger.Discription.OUT, complexEventChunk);
        }
        complexEventChunk.reset();
        while (complexEventChunk.hasNext()) {
            ComplexEvent complexEvent = complexEventChunk.next();
            if (complexEvent.getType() == ComplexEvent.Type.EXPIRED) {
                complexEvent.setType(ComplexEvent.Type.CURRENT);
            }
        }
        publisher.send(complexEventChunk.getFirst());
    }

    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

}

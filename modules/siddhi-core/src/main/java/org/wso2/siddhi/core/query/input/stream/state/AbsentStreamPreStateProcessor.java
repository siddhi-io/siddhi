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

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbsentStreamPreStateProcessor extends StreamPreStateProcessor {
    public AbsentStreamPreStateProcessor(StateInputStream.Type stateType, List<Map.Entry<Long, Set<Integer>>> withinStates) {
        super(stateType, withinStates);
    }


    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        // TODO: 4/6/17
        System.out.println("Process " + complexEventChunk + " in AbsentStreamPreStateProcessor");
        ComplexEventChunk<StateEvent> event = super.processAndReturn(complexEventChunk);
        System.out.println("Return " + event + " in AbsentStreamPreStateProcessor");

        return event;
    }
}

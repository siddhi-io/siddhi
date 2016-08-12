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

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.state.StateEventPool;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.OverwritingStreamEventExtractor;
import org.wso2.siddhi.core.util.collection.UpdateAttributeMapper;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.parser.MatcherParser;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

public class InsertOverwriteTableCallback extends OutputCallback {
    private final int matchingStreamIndex;
    private final UpdateAttributeMapper[] updateAttributeMappers;
    private final OverwritingStreamEventExtractor overwritingStreamEventExtractor;
    private EventTable eventTable;
    private Operator operator;
    private boolean convertToStreamEvent;
    private StateEventPool stateEventPool;
    private StreamEventPool streamEventPool;
    private StreamEventConverter streamEventConvertor;

    public InsertOverwriteTableCallback(EventTable eventTable, Operator operator, AbstractDefinition updatingStreamDefinition,
                                        int matchingStreamIndex, boolean convertToStreamEvent, StateEventPool stateEventPool,
                                        StreamEventPool streamEventPool, StreamEventConverter streamEventConvertor) {
        this.matchingStreamIndex = matchingStreamIndex;
        this.eventTable = eventTable;
        this.operator = operator;
        this.convertToStreamEvent = convertToStreamEvent;
        this.stateEventPool = stateEventPool;
        this.streamEventPool = streamEventPool;
        this.streamEventConvertor = streamEventConvertor;
        this.updateAttributeMappers = MatcherParser.constructUpdateAttributeMapper(eventTable.getTableDefinition(),
                updatingStreamDefinition.getAttributeList(), matchingStreamIndex);
        this.overwritingStreamEventExtractor = new OverwritingStreamEventExtractor(matchingStreamIndex);
    }

    @Override
    public void send(ComplexEventChunk overwriteOrAddEventChunk) {
        overwriteOrAddEventChunk.reset();
        if (overwriteOrAddEventChunk.hasNext()) {
            ComplexEventChunk<StateEvent> overwriteOrAddStateEventChunk = constructMatchingStateEventChunk(overwriteOrAddEventChunk,
                    convertToStreamEvent, stateEventPool, matchingStreamIndex, streamEventPool, streamEventConvertor);
            constructMatchingStateEventChunk(overwriteOrAddEventChunk, convertToStreamEvent, stateEventPool, matchingStreamIndex, streamEventPool, streamEventConvertor);
            eventTable.overwriteOrAdd(overwriteOrAddStateEventChunk, operator, updateAttributeMappers, overwritingStreamEventExtractor);
        }
    }

}

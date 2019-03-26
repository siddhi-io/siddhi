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

package io.siddhi.core.executor.condition;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.FinderStateEvent;
import io.siddhi.core.util.collection.operator.CompiledCondition;

/**
 * Executor class for In condition. Condition evaluation logic is implemented within executor.
 */
public class InConditionExpressionExecutor extends ConditionExpressionExecutor {

    private final int streamEventSize;
    private final boolean isMatchingEventAStateEvent;
    private final int matchingStreamIndex;
    private final CompiledCondition compiledCondition;
    private Table table;

    public InConditionExpressionExecutor(Table table, CompiledCondition compiledCondition, int
            streamEventSize, boolean isMatchingEventAStateEvent, int matchingStreamIndex) {
        this.streamEventSize = streamEventSize;
        this.isMatchingEventAStateEvent = isMatchingEventAStateEvent;
        this.matchingStreamIndex = matchingStreamIndex;
        this.table = table;
        this.compiledCondition = compiledCondition;
    }

    public Boolean execute(ComplexEvent event) {
        FinderStateEvent finderStateEvent = new FinderStateEvent(streamEventSize, 0);
        try {
            if (isMatchingEventAStateEvent) {
                finderStateEvent.setEvent((StateEvent) event);
            } else {
                finderStateEvent.setEvent(matchingStreamIndex, (StreamEvent) event);
            }
            return table.containsEvent(finderStateEvent, compiledCondition);
        } finally {
            if (isMatchingEventAStateEvent) {
                finderStateEvent.setEvent(null);
            } else {
                finderStateEvent.setEvent(matchingStreamIndex, null);
            }
        }
    }

}

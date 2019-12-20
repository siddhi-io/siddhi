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

package io.siddhi.core.query.processor.stream.window;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * Interface for all processors which holds a collection of events and supports traversing and finding events from
 * that collection. find() will be used by Pattern and Join Processors to get matching event.
 */
public interface FindableProcessor {

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent     the event to be matched with the events at the processor
     * @param compiledCondition the execution element responsible for matching the corresponding events that matches
     *                          the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition);

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param condition                   the matching condition
     * @param matchingMetaInfoHolder      the meta structure of the incoming matchingEvent
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param tableMap                    map of event tables
     * @param siddhiQueryContext          current siddhi query context
     * @return compiled Condition having the capability of matching events against the incoming matchingEvent
     */
    CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                       List<VariableExpressionExecutor> variableExpressionExecutors,
                                       Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext);
}

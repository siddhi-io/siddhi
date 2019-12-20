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

package io.siddhi.core.query.processor.stream.window;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.selection.Selector;

import java.util.List;
import java.util.Map;

/**
 * Interface for all processors which holds a collection of events and supports traversing and finding events from
 * that collection with different selection criteria. query() will be used by OnDemandQuery to get matching event in
 * defined format.
 */
public interface QueryableProcessor extends FindableProcessor {

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic and
     * return them based on the defined selection.
     *
     * @param matchingEvent     the event to be matched with the events at the processor
     * @param compiledCondition the execution element responsible for matching the corresponding events that matches
     *                          the matchingEvent based on pool of events at Processor
     * @param compiledSelection the execution element responsible for transforming the corresponding events to the
     *                          given selection
     * @param outputAttributes  the output attributes specified in the query.
     * @return the matched events
     * @throws ConnectionUnavailableException
     */
    StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                      CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException;

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic and
     * return them based on the defined selection.
     *
     * @param matchingEvent     the event to be matched with the events at the processor
     * @param compiledCondition the execution element responsible for matching the corresponding events that matches
     *                          the matchingEvent based on pool of events at Processor
     * @param compiledSelection the execution element responsible for transforming the corresponding events to the
     *                          given selection
     * @return the matched events
     * @throws ConnectionUnavailableException
     */
    @Deprecated
    StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                      CompiledSelection compiledSelection)
            throws ConnectionUnavailableException;

    /**
     * To construct a selection having the capability of transforming events based on given selection logic.
     *
     * @param selector                    the query selector
     * @param expectedOutputAttributes
     * @param matchingMetaInfoHolder      the meta structure of the incoming matchingEvent
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param tableMap                    map of event tables
     * @param siddhiQueryContext          current siddhi query context
     * @return compiled Selection having the capability of transforming events based on the selection
     */
    CompiledSelection compileSelection(Selector selector,
                                       List<Attribute> expectedOutputAttributes,
                                       MatchingMetaInfoHolder matchingMetaInfoHolder,
                                       List<VariableExpressionExecutor> variableExpressionExecutors,
                                       Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext);
}

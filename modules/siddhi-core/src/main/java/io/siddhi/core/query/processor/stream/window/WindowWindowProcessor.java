/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.core.query.processor.stream.window;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.stream.join.JoinProcessor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * This is the {@link WindowProcessor} intended to be used with window join queries.
 * This processor keeps a reference of the {@link Window} and directly find
 * the items from the {@link Window}.
 * The process method just passes the events to the next
 * {@link JoinProcessor} inorder to handle
 * the events there.
 */
public class WindowWindowProcessor extends WindowProcessor implements FindableProcessor {

    /**
     * {@link Window} from which the events have to be found.
     */
    private Window window;

    public WindowWindowProcessor(Window window) {
        this.window = window;
    }

    @Override
    protected StateFactory init(MetaStreamEvent metaStreamEvent,
                                AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, StreamEventClonerHolder streamEventClonerHolder,
                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                SiddhiQueryContext siddhiQueryContext) {
        return null;
    }

    @Override
    protected void processEventChunk(ComplexEventChunk streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner,
                                     ComplexEventPopulater complexEventPopulater, State state) {
        streamEventChunk.reset();
        nextProcessor.process(streamEventChunk);
    }


    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return window.find(matchingEvent, compiledCondition);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        return window.compileCondition(condition, matchingMetaInfoHolder, variableExpressionExecutors, tableMap,
                siddhiQueryContext);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return window.getProcessingMode();
    }

}

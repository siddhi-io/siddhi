/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.stream.join.JoinProcessor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;

import java.util.List;
import java.util.Map;

/**
 * This is the {@link WindowProcessor} intended to be used with aggregate join queries.
 * This processor keeps a reference of the Aggregate and finds the items from that.
 * The process method just passes the events to the next
 * {@link JoinProcessor} inorder to handle
 * the events there.
 */
public class AggregateWindowProcessor extends BatchingWindowProcessor implements FindableProcessor {
    private final Within within;
    private final Expression per;
    private List<Variable> queryGroupByList;
    private AggregationRuntime aggregationRuntime;

    public AggregateWindowProcessor(AggregationRuntime aggregationRuntime, Within within, Expression per,
                                    List<Variable> queryGroupByList) {
        this.aggregationRuntime = aggregationRuntime;
        this.within = within;
        this.per = per;
        this.queryGroupByList = queryGroupByList;
    }

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean outputExpectsExpiredEvents,
                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        // nothing to be done
        return null;
    }

    @Override
    protected void process(ComplexEventChunk streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, State state) {
        // Pass the event  to the post JoinProcessor
        nextProcessor.process(streamEventChunk);
    }


    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return aggregationRuntime.find(matchingEvent, compiledCondition, siddhiQueryContext);

    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        return aggregationRuntime.compileExpression(condition, within, per, queryGroupByList,
                matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }
}

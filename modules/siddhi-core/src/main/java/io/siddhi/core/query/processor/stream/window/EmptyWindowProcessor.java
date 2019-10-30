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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Batch Window operating based on pre-defined length 0.
 */
public class EmptyWindowProcessor extends
        BatchingFindableWindowProcessor<EmptyWindowProcessor.WindowState> {

    private boolean outputExpectsExpiredEvents;
    private SiddhiQueryContext siddhiQueryContext;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean outputExpectsExpiredEvents,
                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.siddhiQueryContext = siddhiQueryContext;
        return () -> new EmptyWindowProcessor.WindowState(streamEventClonerHolder, false,
                outputExpectsExpiredEvents, findToBeExecuted);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        List<ComplexEventChunk> streamEventChunks = new LinkedList<>();
        synchronized (state) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>();
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEventChunk.remove();
                outputStreamEventChunk.add(streamEvent);
                if (outputExpectsExpiredEvents) {
                    StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                    expiredEvent.setTimestamp(currentTime);
                    outputStreamEventChunk.add(expiredEvent);
                }
                StreamEvent resetEvent = streamEventCloner.copyStreamEvent(streamEvent);
                resetEvent.setType(ComplexEvent.Type.RESET);
                resetEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(resetEvent);
                if (outputStreamEventChunk.getFirst() != null) {
                    streamEventChunks.add(outputStreamEventChunk);
                    outputStreamEventChunk = new ComplexEventChunk<>();
                }
            }
        }
        nextProcessor.process(streamEventChunks);
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
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(state.expiredEventQueue, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventQueue, streamEventCloner);
    }

    class WindowState extends State {

        private SnapshotableStreamEventQueue expiredEventQueue = null;

        public WindowState(StreamEventClonerHolder streamEventClonerHolder, boolean isStreamCurrentEvents,
                           boolean outputExpectsExpiredEvents, boolean findToBeExecuted) {
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
        }

        @Override
        public boolean canDestroy() {
            return (expiredEventQueue == null || expiredEventQueue.getFirst() == null);
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("ExpiredEventQueue", expiredEventQueue != null ? expiredEventQueue.getSnapshot() : null);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            if (expiredEventQueue != null) {
                expiredEventQueue.clear();
                expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            }
        }
    }
}

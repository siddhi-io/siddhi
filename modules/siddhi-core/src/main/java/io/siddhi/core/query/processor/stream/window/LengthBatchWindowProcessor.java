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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ConstantExpressionExecutor;
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
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Batch Window operating based on pre-defined length.
 */
@Extension(
        name = "lengthBatch",
        namespace = "",
        description = "A batch (tumbling) length window that holds and process a number of events as specified in " +
                "the window.length.",
        parameters = {
                @Parameter(name = "window.length",
                        description = "The number of events the window should tumble.",
                        type = {DataType.INT}),
                @Parameter(name = "stream.current.event",
                        description = "Let the window stream the current events out as and when they arrive" +
                                " to the window while expiring them in batches.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"window.length"}),
                @ParameterOverload(parameterNames = {"window.length", "stream.current.event"})
        },
        examples = {
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from InputEventStream#lengthBatch(10)\n" +
                                "select symbol, sum(price) as price \n" +
                                "insert into OutputStream;",
                        description = "This collect and process 10 events as a batch" +
                                " and output them."
                ),
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from InputEventStream#lengthBatch(10, true)\n" +
                                "select symbol, sum(price) as sumPrice \n" +
                                "insert into OutputStream;",
                        description = "This window sends the arriving events directly to the output letting the " +
                                "`sumPrice` to increase gradually, after every 10 events it clears the " +
                                "window as a batch and resets the `sumPrice` to zero."
                ),
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n" +
                                "define window StockEventWindow (symbol string, price float, volume int) " +
                                "lengthBatch(10) output all events;\n\n" +
                                "@info(name = 'query0')\n" +
                                "from InputEventStream\n" +
                                "insert into StockEventWindow;\n\n" +
                                "@info(name = 'query1')\n" +
                                "from StockEventWindow\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert all events into OutputStream ;",
                        description = "This uses an defined window to process 10 events " +
                                " as a batch and output all events."
                )
        }
)
public class LengthBatchWindowProcessor extends
        BatchingFindableWindowProcessor<LengthBatchWindowProcessor.WindowState> {

    private int length;
    private boolean outputExpectsExpiredEvents;
    private SiddhiQueryContext siddhiQueryContext;
    private boolean isStreamCurrentEvents = false;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean outputExpectsExpiredEvents,
                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.siddhiQueryContext = siddhiQueryContext;
        if (attributeExpressionExecutors.length >= 1) {
            if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (1st) parameter " +
                        "'window.length' should be a constant but found a dynamic parameter " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
        }
        if (attributeExpressionExecutors.length == 2) {
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (2nd) parameter " +
                        "'stream.current.event' should be a constant but found a dynamic parameter " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            isStreamCurrentEvents = (Boolean) (((ConstantExpressionExecutor)
                    attributeExpressionExecutors[1]).getValue());
        }
        if (attributeExpressionExecutors.length > 2) {
            throw new SiddhiAppValidationException("LengthBatch window should have one parameter (<int> " +
                    "window.length) or two parameters (<int> window.length, <bool> stream.current.event), " +
                    "but found " + attributeExpressionExecutors.length + " input parameters.");
        }
        return () -> new WindowState(streamEventClonerHolder, isStreamCurrentEvents,
                outputExpectsExpiredEvents, findToBeExecuted);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<>();
        synchronized (state) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<>();
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEventChunk.remove();
                if (length == 0) {
                    processLengthZeroBatch(streamEvent, outputStreamEventChunk, currentTime, streamEventCloner);
                } else {
                    if (state.resetEvent == null) {
                        state.resetEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        state.resetEvent.setType(ComplexEvent.Type.RESET);
                    }
                    if (isStreamCurrentEvents) {
                        processStreamCurrentEvents(streamEvent, outputStreamEventChunk, currentTime, state,
                                streamEventCloner);
                    } else {
                        processFullBatchEvents(streamEvent, outputStreamEventChunk, currentTime, state,
                                streamEventCloner);
                    }
                }
                if (outputStreamEventChunk.getFirst() != null) {
                    streamEventChunks.add(outputStreamEventChunk);
                    outputStreamEventChunk = new ComplexEventChunk<>();
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    private void processLengthZeroBatch(StreamEvent streamEvent,
                                        ComplexEventChunk<StreamEvent> outputStreamEventChunk,
                                        long currentTime, StreamEventCloner streamEventCloner) {

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
    }

    private void processFullBatchEvents(StreamEvent streamEvent,
                                        ComplexEventChunk<StreamEvent> outputStreamEventChunk,
                                        long currentTime, WindowState state, StreamEventCloner streamEventCloner) {
        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
        state.currentEventQueue.add(clonedStreamEvent);
        state.count++;
        if (state.count == length) {
            if (outputExpectsExpiredEvents && state.expiredEventQueue.getFirst() != null) {
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(state.expiredEventQueue.getFirst());
                state.expiredEventQueue.clear();
            }

            if (state.resetEvent != null) {
                state.resetEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(state.resetEvent);
                state.resetEvent = null;
            }

            if (state.currentEventQueue.getFirst() != null) {
                if (state.expiredEventQueue != null) {
                    state.currentEventQueue.reset();
                    while (state.currentEventQueue.hasNext()) {
                        StreamEvent currentEvent = state.currentEventQueue.next();
                        StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                        toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                        state.expiredEventQueue.add(toExpireEvent);
                    }
                }
                outputStreamEventChunk.add(state.currentEventQueue.getFirst());
                state.currentEventQueue.clear();
            }
            state.count = 0;
        }
    }

    private void processStreamCurrentEvents(StreamEvent streamEvent,
                                            ComplexEventChunk<StreamEvent> outputStreamEventChunk,
                                            long currentTime, WindowState state,
                                            StreamEventCloner streamEventCloner) {
        state.count++;
        if (state.count == length + 1) {
            if (outputExpectsExpiredEvents && state.expiredEventQueue.getFirst() != null) {
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(state.expiredEventQueue.getFirst());
                state.expiredEventQueue.clear();
            }

            if (state.resetEvent != null) {
                state.resetEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(state.resetEvent);
                state.resetEvent = null;
            }
            state.count = 1;
        }
        outputStreamEventChunk.add(streamEvent);
        if (state.expiredEventQueue != null) {
            StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
            clonedStreamEvent.setType(StreamEvent.Type.EXPIRED);
            state.expiredEventQueue.add(clonedStreamEvent);
        }

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

        private int count = 0;
        private SnapshotableStreamEventQueue currentEventQueue = null;
        private SnapshotableStreamEventQueue expiredEventQueue = null;
        private StreamEvent resetEvent = null;

        public WindowState(StreamEventClonerHolder streamEventClonerHolder, boolean isStreamCurrentEvents,
                           boolean outputExpectsExpiredEvents, boolean findToBeExecuted) {
            if (!isStreamCurrentEvents) {
                currentEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
        }

        @Override
        public boolean canDestroy() {
            return (currentEventQueue == null || currentEventQueue.getFirst() == null) &&
                    (expiredEventQueue == null || expiredEventQueue.getFirst() == null) &&
                    resetEvent == null && count == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Count", count);
            state.put("CurrentEventQueue", currentEventQueue != null ? currentEventQueue.getSnapshot() : null);
            state.put("ExpiredEventQueue", expiredEventQueue != null ? expiredEventQueue.getSnapshot() : null);
            state.put("ResetEvent", resetEvent);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            count = (int) state.get("Count");
            if (currentEventQueue != null) {
                currentEventQueue.clear();
                currentEventQueue.restore((SnapshotStateList) state.get("CurrentEventQueue"));
            }

            if (expiredEventQueue != null) {
                expiredEventQueue.clear();
                expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            }
            resetEvent = (StreamEvent) state.get("ResetEvent");
        }
    }
}

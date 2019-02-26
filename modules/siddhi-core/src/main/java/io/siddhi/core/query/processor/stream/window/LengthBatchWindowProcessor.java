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
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
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
public class LengthBatchWindowProcessor extends BatchingWindowProcessor implements FindableProcessor {

    private int length;
    private int count = 0;
    private SnapshotableStreamEventQueue currentEventQueue = null;
    private SnapshotableStreamEventQueue expiredEventQueue = null;
    private boolean outputExpectsExpiredEvents;
    private SiddhiQueryContext siddhiQueryContext;
    private boolean isStreamCurrentEvents = false;
    private StreamEvent resetEvent = null;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean outputExpectsExpiredEvents, SiddhiQueryContext siddhiQueryContext) {
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
        if (!isStreamCurrentEvents) {
            currentEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }
        if (outputExpectsExpiredEvents) {
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEventChunk.remove();
                if (length == 0) {
                    processLengthZeroBatch(streamEvent, outputStreamEventChunk, currentTime);
                } else {
                    if (resetEvent == null) {
                        resetEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        resetEvent.setType(ComplexEvent.Type.RESET);
                    }
                    if (isStreamCurrentEvents) {
                        processStreamCurrentEvents(streamEvent, outputStreamEventChunk, currentTime);
                    } else {
                        processFullBatchEvents(streamEvent, outputStreamEventChunk, currentTime);
                    }
                }
                if (outputStreamEventChunk.getFirst() != null) {
                    streamEventChunks.add(outputStreamEventChunk);
                    outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    private void processLengthZeroBatch(StreamEvent streamEvent,
                                        ComplexEventChunk<StreamEvent> outputStreamEventChunk,
                                        long currentTime) {

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
                                        long currentTime) {
        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
        currentEventQueue.add(clonedStreamEvent);
        count++;
        if (count == length) {
            if (outputExpectsExpiredEvents && expiredEventQueue.getFirst() != null) {
                while (expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(expiredEventQueue.getFirst());
                expiredEventQueue.clear();
            }

            if (resetEvent != null) {
                resetEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(resetEvent);
                resetEvent = null;
            }

            if (currentEventQueue.getFirst() != null) {
                if (expiredEventQueue != null) {
                    currentEventQueue.reset();
                    while (currentEventQueue.hasNext()) {
                        StreamEvent currentEvent = currentEventQueue.next();
                        StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                        toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                        expiredEventQueue.add(toExpireEvent);
                    }
                }
                outputStreamEventChunk.add(currentEventQueue.getFirst());
                currentEventQueue.clear();
            }
            count = 0;
        }
    }

    private void processStreamCurrentEvents(StreamEvent streamEvent,
                                            ComplexEventChunk<StreamEvent> outputStreamEventChunk,
                                            long currentTime) {
        count++;
        if (count == length + 1) {
            if (outputExpectsExpiredEvents && expiredEventQueue.getFirst() != null) {
                while (expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(expiredEventQueue.getFirst());
                expiredEventQueue.clear();
            }

            if (resetEvent != null) {
                resetEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(resetEvent);
                resetEvent = null;
            }
            count = 1;
        }
        outputStreamEventChunk.add(streamEvent);
        if (expiredEventQueue != null) {
            StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
            clonedStreamEvent.setType(StreamEvent.Type.EXPIRED);
            expiredEventQueue.add(clonedStreamEvent);
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
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        synchronized (this) {
            state.put("Count", count);
            state.put("CurrentEventQueue", currentEventQueue != null ? currentEventQueue.getSnapshot() : null);
            state.put("ExpiredEventQueue", expiredEventQueue != null ? expiredEventQueue.getSnapshot() : null);
            state.put("ResetEvent", resetEvent);
        }
        return state;
    }


    @Override
    public synchronized void restoreState(Map<String, Object> state) {
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

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return ((Operator) compiledCondition).find(matchingEvent, expiredEventQueue, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        if (expiredEventQueue == null) {
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }
        return OperatorParser.constructOperator(expiredEventQueue, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }
}

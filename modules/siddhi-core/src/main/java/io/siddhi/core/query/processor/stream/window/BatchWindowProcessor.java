/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * Implementation of {@link WindowProcessor} which represent a Batch Window that aggregate batch of incoming events
 * together.
 */
@Extension(
        name = "batch",
        namespace = "",
        description = "A window that holds an incoming events batch. When a new set of events arrives, the previously" +
                " arrived old events will be expired. Batch window can be used to aggregate events that comes " +
                "in batches. If it has the parameter length specified, then batch window process the batch " +
                "as several chunks.",
        parameters = {
                @Parameter(name = "window.length",
                        description = "The length of a chunk",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "If length value was not given it assign 0 as length and process " +
                                "the whole batch as once"
                ),
        },
        parameterOverloads = {
                @ParameterOverload(),
                @ParameterOverload(parameterNames = {"window.length"})
        },
        examples = {
                @Example(
                        syntax =
                                "define stream consumerItemStream (itemId string, price float)\n\n" +
                                        "from consumerItemStream#window.batch()\n" +
                                        "select price, str:groupConcat(itemId) as itemIds\n" +
                                        "group by price\n" +
                                        "insert into outputStream;",
                        description = "This will output comma separated items IDs that have the same price for each " +
                                "incoming batch of events."
                )
        }
)
public class BatchWindowProcessor extends BatchingFindableWindowProcessor<BatchWindowProcessor.WindowState> {

    private int length = 0;
    private boolean outputExpectsExpiredEvents;
    private boolean findToBeExecuted;
    private SiddhiQueryContext siddhiQueryContext;

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader,
                                             StreamEventClonerHolder streamEventClonerHolder,
                                             boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                             SiddhiQueryContext siddhiQueryContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.findToBeExecuted = findToBeExecuted;
        this.siddhiQueryContext = siddhiQueryContext;
        if (attributeExpressionExecutors.length == 1) {
            length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
        } else if (attributeExpressionExecutors.length == 0) {
            length = 0;
        } else {
            throw new SiddhiAppValidationException("Batch window should have at most one parameter (<int> " +
                    "chunkLength), but found " + attributeExpressionExecutors.length + " input attributes");
        }
        if (length < 0) {
            throw new SiddhiAppValidationException("Batch window should have at most one parameter (<int> " +
                    "chunkLength) greater than zero. But found value 'chunkLength = " + length + " ' ");
        }
        return () -> new WindowState(streamEventClonerHolder, outputExpectsExpiredEvents, findToBeExecuted);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>();
        synchronized (state) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            if (outputExpectsExpiredEvents) {
                if (state.expiredEventQueue.getFirst() != null) {
                    while (state.expiredEventQueue.hasNext()) {
                        state.expiredEventQueue.next().setTimestamp(currentTime);
                    }
                    currentEventChunk.add(state.expiredEventQueue.getFirst());
                    if (state.resetEvent != null) {
                        currentEventChunk.add(state.resetEvent);
                        state.resetEvent = null;
                    }
                }
                state.expiredEventQueue.clear();
            }
            //check whether the streamEventChunk has next event before add into output stream event chunk
            ComplexEventChunk<StreamEvent> currentEventQueue = new ComplexEventChunk<>();
            int count = 0;
            if (streamEventChunk.hasNext()) {
                do {
                    StreamEvent streamEvent = streamEventChunk.next();
                    StreamEvent clonedStreamEventToProcess = streamEventCloner.copyStreamEvent(streamEvent);
                    if (outputExpectsExpiredEvents) {
                        StreamEvent clonedStreamEventToExpire = streamEventCloner.copyStreamEvent(streamEvent);
                        clonedStreamEventToExpire.setType(StreamEvent.Type.EXPIRED);
                        state.expiredEventQueue.add(clonedStreamEventToExpire);
                    }
                    currentEventQueue.add(clonedStreamEventToProcess);
                    count++;
                    if (count == length) {
                        if (currentEventQueue.getFirst() != null) {
                            if (state.resetEvent != null) {
                                currentEventChunk.add(state.resetEvent);
                            }
                            state.resetEvent = streamEventCloner.copyStreamEvent(currentEventQueue.getFirst());
                            state.resetEvent.setType(ComplexEvent.Type.RESET);
                            currentEventChunk.add(currentEventQueue.getFirst());
                        }
                        count = 0;
                        currentEventQueue.clear();
                        if (currentEventChunk.getFirst() != null) {
                            streamEventChunks.add(currentEventChunk);
                            currentEventChunk = new ComplexEventChunk<StreamEvent>();
                        }
                    }
                } while (streamEventChunk.hasNext());
                if (currentEventQueue.getFirst() != null) {
                    if (state.resetEvent != null) {
                        currentEventChunk.add(state.resetEvent);
                    }
                    state.resetEvent = streamEventCloner.copyStreamEvent(currentEventQueue.getFirst());
                    state.resetEvent.setType(ComplexEvent.Type.RESET);
                    currentEventChunk.add(currentEventQueue.getFirst());
                }
                if (currentEventChunk.getFirst() != null) {
                    streamEventChunks.add(currentEventChunk);
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> processStreamEventChunk : streamEventChunks) {
            nextProcessor.process(processStreamEventChunk);
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
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventQueue, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(state.expiredEventQueue, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    class WindowState extends State {
        private SnapshotableStreamEventQueue expiredEventQueue;
        private StreamEvent resetEvent = null;

        WindowState(StreamEventClonerHolder streamEventClonerHolder,
                    boolean outputExpectsExpiredEvents, boolean findToBeExecuted) {
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();

            if (outputExpectsExpiredEvents || findToBeExecuted) {
                state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            }
            state.put("ResetEvent", resetEvent);
            return state;
        }

        public void restore(Map<String, Object> state) {
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                expiredEventQueue.clear();
                expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            }
            resetEvent = (StreamEvent) state.get("ResetEvent");
        }

        @Override
        public boolean canDestroy() {
            return expiredEventQueue.getFirst() == null && resetEvent == null;
        }
    }
}

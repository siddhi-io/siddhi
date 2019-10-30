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
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Batch Window operating based on time.
 */
@Extension(
        name = "timeBatch",
        namespace = "",
        description = "A batch (tumbling) time window that holds and process events that arrive during " +
                "'window.time' period as a batch.",
        parameters = {
                @Parameter(name = "window.time",
                        description = "The batch time period in which the window process the events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "start.time",
                        description = "This specifies an offset in milliseconds in order to start the " +
                                "window at a time different to the standard time.",
                        type = {DataType.INT, DataType.LONG},
                        optional = true,
                        defaultValue = "Timestamp of first event"),
                @Parameter(name = "stream.current.event",
                        description = "Let the window stream the current events out as and when they arrive to the " +
                                "window while expiring them in batches.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"window.time"}),
                @ParameterOverload(parameterNames = {"window.time", "start.time"}),
                @ParameterOverload(parameterNames = {"window.time", "stream.current.event"}),
                @ParameterOverload(parameterNames = {"window.time", "start.time", "stream.current.event"})
        },
        examples = {
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from InputEventStream#timeBatch(20 sec)\n" +
                                "select symbol, sum(price) as price \n" +
                                "insert into OutputStream;",
                        description = "This collect and process incoming events as a batch every 20 seconds" +
                                " and output them."
                ),
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from InputEventStream#timeBatch(20 sec, true)\n" +
                                "select symbol, sum(price) as sumPrice \n" +
                                "insert into OutputStream;",
                        description = "This window sends the arriving events directly to the output letting the " +
                                "`sumPrice` to increase gradually and on every 20 second interval it clears the " +
                                "window as a batch resetting the `sumPrice` to zero."
                ),
                @Example(
                        syntax = "define stream InputEventStream (symbol string, price float, volume int);\n" +
                                "define window StockEventWindow (symbol string, price float, volume int) " +
                                "timeBatch(20 sec) output all events;\n\n" +
                                "@info(name = 'query0')\n" +
                                "from InputEventStream\n" +
                                "insert into StockEventWindow;\n\n" +
                                "@info(name = 'query1')\n" +
                                "from StockEventWindow\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert all events into OutputStream ;",
                        description = "This uses an defined window to process events arrived every 20 seconds" +
                                " as a batch and output all events."
                )
        }
)
public class TimeBatchWindowProcessor extends BatchingFindableWindowProcessor<TimeBatchWindowProcessor.WindowState>
        implements SchedulingProcessor, FindableProcessor {

    private long timeInMilliSeconds;
    private long nextEmitTime = -1;
    private Scheduler scheduler;
    private boolean outputExpectsExpiredEvents;
    private SiddhiQueryContext siddhiQueryContext;
    private boolean isStartTimeEnabled = false;
    private boolean isStreamCurrentEvents = false;
    private long startTime = 0;

    public void setTimeInMilliSeconds(long timeInMilliSeconds) {
        this.timeInMilliSeconds = timeInMilliSeconds;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean
                                        outputExpectsExpiredEvents, boolean findToBeExecuted,
                                SiddhiQueryContext siddhiQueryContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.siddhiQueryContext = siddhiQueryContext;
        if (attributeExpressionExecutors.length == 1) {
            initTimeParameter(attributeExpressionExecutors[0]);
        } else if (attributeExpressionExecutors.length == 2) {
            initTimeParameter(attributeExpressionExecutors[0]);

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (2nd) parameter should be " +
                        "constant but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            // start time
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                isStartTimeEnabled = true;
                startTime = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                isStartTimeEnabled = true;
                startTime = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.BOOL) {
                isStreamCurrentEvents = Boolean.valueOf(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else {
                throw new SiddhiAppValidationException("TimeBatch window's 2nd parameter " +
                        "should be 'start.time' which is int or long, or 'stream.current.event' which is bool " +
                        " but found " + attributeExpressionExecutors[1].getReturnType());
            }
        } else if (attributeExpressionExecutors.length == 3) {
            initTimeParameter(attributeExpressionExecutors[0]);

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (2nd) parameter 'start.time' " +
                        "should be a constant but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            // start time
            isStartTimeEnabled = true;
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                startTime = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                startTime = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else {
                throw new SiddhiAppValidationException("TimeBatch window's 2nd parameter " +
                        "should be 'start.time' which is int or long, " +
                        " but found " + attributeExpressionExecutors[1].getReturnType());
            }

            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (3rd) parameter " +
                        "'stream.current.event' should be a constant but found a dynamic attribute " +
                        attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL) {
                isStreamCurrentEvents = Boolean.valueOf(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[2]).getValue()));
            } else {
                throw new SiddhiAppValidationException("TimeBatch window's 3rd parameter " +
                        "should be 'stream.current.event' which is bool " +
                        " but found " + attributeExpressionExecutors[2].getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("Time window should only have one or two parameters. " +
                    "(<int|long|time> windowTime), but found " +
                    attributeExpressionExecutors.length + " input " +
                    "attributes");
        }
        return () -> new WindowState(streamEventClonerHolder, outputExpectsExpiredEvents, findToBeExecuted);

    }

    @Override
    public ProcessingMode getProcessingMode() {
        if (isStreamCurrentEvents) {
            return ProcessingMode.RESET;
        } else {
            return ProcessingMode.BATCH;
        }
    }


    private void initTimeParameter(ExpressionExecutor attributeExpressionExecutor) {
        if (attributeExpressionExecutor instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutor.getReturnType() == Attribute.Type.INT) {
                timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutor)
                        .getValue();

            } else if (attributeExpressionExecutor.getReturnType() == Attribute.Type.LONG) {
                timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutor)
                        .getValue();
            } else {
                throw new SiddhiAppValidationException("TimeBatch window's window.time (1st) parameter 'window.time' " +
                        "should be either int or long, but found " + attributeExpressionExecutor.getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("TimeBatch window's window.time (1st) parameter 'window.time' " +
                    "should be a constant but found a dynamic attribute " +
                    attributeExpressionExecutor.getClass().getCanonicalName());
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        synchronized (state) {
            if (nextEmitTime == -1) {
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime()
                            + timeInMilliSeconds;
                }
                scheduler.notifyAt(nextEmitTime);
            }
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            boolean sendEvents;

            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }

            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                if (state.resetEvent == null) {
                    state.resetEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    state.resetEvent.setType(ComplexEvent.Type.RESET);
                }
                if (!isStreamCurrentEvents) {
                    state.currentEventQueue.add(clonedStreamEvent);
                } else if (state.expiredEventQueue != null) {
                    clonedStreamEvent.setType(StreamEvent.Type.EXPIRED);
                    state.expiredEventQueue.add(clonedStreamEvent);
                }
            }
            if (!isStreamCurrentEvents) {
                streamEventChunk.clear();
            }
            if (sendEvents) {
                if (outputExpectsExpiredEvents && state.expiredEventQueue.getFirst() != null) {
                    while (state.expiredEventQueue.hasNext()) {
                        StreamEvent expiredEvent = state.expiredEventQueue.next();
                        expiredEvent.setTimestamp(currentTime);
                    }
                    streamEventChunk.add(state.expiredEventQueue.getFirst());
                    state.expiredEventQueue.clear();
                }

                if (state.resetEvent != null) {
                    streamEventChunk.add(state.resetEvent);
                    state.resetEvent = null;
                }

                if (state.currentEventQueue != null && state.currentEventQueue.getFirst() != null) {
                    if (state.expiredEventQueue != null) {
                        state.currentEventQueue.reset();
                        while (state.currentEventQueue.hasNext()) {
                            StreamEvent currentEvent = state.currentEventQueue.next();
                            StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                            toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                            state.expiredEventQueue.add(toExpireEvent);
                        }
                    }
                    streamEventChunk.add(state.currentEventQueue.getFirst());
                    state.currentEventQueue.clear();
                }
            }
        }
        if (streamEventChunk.getFirst() != null) {
            nextProcessor.process(streamEventChunk);
        }
    }

    private long getNextEmitTime(long currentTime) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeInMilliSeconds;
        long emitTime = currentTime + (timeInMilliSeconds - elapsedTimeSinceLastEmit);
        return emitTime;
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.stop();
        }
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
        private SnapshotableStreamEventQueue currentEventQueue;
        private SnapshotableStreamEventQueue expiredEventQueue;
        private StreamEvent resetEvent = null;

        WindowState(StreamEventClonerHolder streamEventClonerHolder,
                    boolean outputExpectsExpiredEvents, boolean findToBeExecuted) {
            if (!isStreamCurrentEvents) {
                this.currentEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                this.expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
            }
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("CurrentEventQueue", currentEventQueue != null ? currentEventQueue.getSnapshot() : null);
            state.put("ExpiredEventQueue", expiredEventQueue != null ? expiredEventQueue.getSnapshot() : null);
            state.put("ResetEvent", resetEvent);
            return state;
        }

        public void restore(Map<String, Object> state) {
            if (expiredEventQueue != null) {
                expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            }
            if (currentEventQueue != null) {
                currentEventQueue.restore((SnapshotStateList) state.get("CurrentEventQueue"));
            }
            resetEvent = (StreamEvent) state.get("ResetEvent");
        }

        @Override
        public boolean canDestroy() {
            return (currentEventQueue == null || currentEventQueue.getFirst() == null)
                    && (expiredEventQueue == null || expiredEventQueue.getFirst() == null)
                    && resetEvent == null;
        }
    }
}

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
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Batch Window operating based on external time.
 */
@Extension(
        name = "externalTimeBatch",
        namespace = "",
        description = "A batch (tumbling) time window based on external time, that holds events arrived " +
                "during windowTime periods, and gets updated for every windowTime.",
        parameters = {
                @Parameter(name = "timestamp",
                        description = "The time which the window determines as current time and will act upon. " +
                                "The value of this parameter should be monotonically increasing.",
                        type = {DataType.LONG},
                        dynamic = true),
                @Parameter(name = "window.time",
                        description = "The batch time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "start.time",
                        description = "User defined start time. This could either be a constant (of type int, " +
                                "long or time) or an attribute of the corresponding stream (of type long). " +
                                "If an attribute is provided, initial value of attribute would be considered as " +
                                "startTime.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME},
                        optional = true,
                        dynamic = true,
                        defaultValue = "Timestamp of first event"),
                @Parameter(name = "timeout",
                        description = "Time to wait for arrival of new event, before flushing " +
                                "and giving output for events belonging to a specific batch.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME},
                        optional = true,
                        defaultValue = "System waits till an event from next batch arrives to flush current batch"),
                @Parameter(name = "replace.with.batchtime",
                        description = "This indicates to replace the expired event timeStamp as the batch end " +
                                "timeStamp",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "System waits till an event from next batch arrives to flush current batch")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"timestamp", "window.time"}),
                @ParameterOverload(parameterNames = {"timestamp", "window.time", "start.time"}),
                @ParameterOverload(parameterNames = {"timestamp", "window.time", "start.time", "timeout"}),
                @ParameterOverload(parameterNames = {"timestamp", "window.time", "start.time", "timeout",
                        "replace.with.batchtime"})
        },
        examples = {
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "externalTimeBatch(eventTime, 1 sec) output expired events;\n" +
                                "@info(name = 'query0')\n" +
                                "from cseEventStream\n" +
                                "insert into cseEventWindow;\n" +
                                "@info(name = 'query1')\n" +
                                "from cseEventWindow\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert expired events into outputStream ;",
                        description = "This will processing events that arrive every 1 seconds " +
                                "from the eventTime."
                ),
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "externalTimeBatch(eventTime, 20 sec, 0) output expired events;",
                        description = "This will processing events that arrive every 1 seconds " +
                                "from the eventTime. Starts on 0th millisecond of an hour."
                ),
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "externalTimeBatch(eventTime, 2 sec, eventTimestamp, 100) output expired events;",
                        description = "This will processing events that arrive every 2 seconds from the " +
                                "eventTim. Considers the first event's eventTimestamp value as startTime. " +
                                "Waits 100 milliseconds for the arrival of a new event before flushing current batch."
                )
        }
)
public class ExternalTimeBatchWindowProcessor
        extends BatchingFindableWindowProcessor<ExternalTimeBatchWindowProcessor.WindowState>
        implements SchedulingProcessor {
    private VariableExpressionExecutor timestampExpressionExecutor;
    private ExpressionExecutor startTimeAsVariable;
    private long timeToKeep;
    private boolean isStartTimeEnabled = false;
    private long schedulerTimeout = 0;
    private Scheduler scheduler;
    private boolean findToBeExecuted = false;
    private boolean replaceTimestampWithBatchEndTime = false;
    private boolean outputExpectsExpiredEvents;
    private long commonStartTime = 0;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder,
                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                SiddhiQueryContext siddhiQueryContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.findToBeExecuted = findToBeExecuted;

        if (attributeExpressionExecutors.length >= 2 && attributeExpressionExecutors.length <= 5) {

            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException("ExternalTime window's 1st parameter timestamp should be a" +
                        " variable, but found " + attributeExpressionExecutors[0].getClass());
            }
            if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppValidationException("ExternalTime window's 1st parameter timestamp should be " +
                        "type long, but found " + attributeExpressionExecutors[0].getReturnType());
            }
            timestampExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                timeToKeep = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

            } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                timeToKeep = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppValidationException("ExternalTimeBatch window's 2nd parameter windowTime " +
                        "should be either int or long, but found " + attributeExpressionExecutors[1].getReturnType());
            }

            if (attributeExpressionExecutors.length >= 3) {
                isStartTimeEnabled = true;
                if ((attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                        commonStartTime = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue()));
                    } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                        commonStartTime = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue()));
                    } else {
                        throw new SiddhiAppValidationException("ExternalTimeBatch window's 3rd parameter " +
                                "startTime should either be a constant (of type int or long) or an attribute (of type" +
                                " long), but found " + attributeExpressionExecutors[2].getReturnType());
                    }
                } else if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.LONG) {
                    throw new SiddhiAppValidationException("ExternalTimeBatch window's 3rd parameter startTime " +
                            "should either be a constant (of type int or long) or an attribute (of type long), but " +
                            "found " + attributeExpressionExecutors[2].getReturnType());
                } else {
                    startTimeAsVariable = attributeExpressionExecutors[2];
                }
            }

            if (attributeExpressionExecutors.length >= 4) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                    schedulerTimeout = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue()));
                } else if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                    schedulerTimeout = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue()));
                } else {
                    throw new SiddhiAppValidationException("ExternalTimeBatch window's 4th parameter timeout " +
                            "should be either int or long, but found " + attributeExpressionExecutors[3]
                            .getReturnType());
                }
            }

            if (attributeExpressionExecutors.length == 5) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.BOOL) {
                    replaceTimestampWithBatchEndTime = Boolean.parseBoolean(String.valueOf((
                            (ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue()));
                } else {
                    throw new SiddhiAppValidationException("ExternalTimeBatch window's 5th parameter " +
                            "replaceTimestampWithBatchEndTime should be bool, but found " +
                            attributeExpressionExecutors[4].getReturnType());
                }
            }
        } else {
            throw new SiddhiAppValidationException("ExternalTimeBatch window should only have two to five " +
                    "parameters (<long> timestamp, <int|long|time> windowTime, <long> startTime, <int|long|time> " +
                    "timeout, <bool> replaceTimestampWithBatchEndTime), but found " + attributeExpressionExecutors
                    .length + " input attributes");
        }
        return () -> new WindowState(outputExpectsExpiredEvents, schedulerTimeout, commonStartTime);
    }

    /**
     * Here an assumption is taken:
     * Parameter: timestamp: The time which the window determines as current time and will act upon,
     * the value of this parameter should be monotonically increasing.
     * from https://docs.wso2.com/display/CEP400/Inbuilt+Windows#InbuiltWindows-externalTime
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {

        // event incoming trigger process. No events means no action
        if (streamEventChunk.getFirst() == null) {
            return;
        }

        List<ComplexEventChunk<StreamEvent>> complexEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (state) {
            initTiming(streamEventChunk.getFirst(), state);

            StreamEvent nextStreamEvent = streamEventChunk.getFirst();
            while (nextStreamEvent != null) {

                StreamEvent currStreamEvent = nextStreamEvent;
                nextStreamEvent = nextStreamEvent.getNext();

                if (currStreamEvent.getType() == ComplexEvent.Type.TIMER) {
                    if (state.lastScheduledTime <= currStreamEvent.getTimestamp()) {
                        // implies that there have not been any more events after this schedule has been done.
                        if (!state.flushed) {
                            flushToOutputChunk(streamEventCloner, complexEventChunks, state.lastCurrentEventTime,
                                    true, state);
                            state.flushed = true;
                        } else {
                            if (state.currentEventChunk.getFirst() != null) {
                                appendToOutputChunk(streamEventCloner, complexEventChunks, state.lastCurrentEventTime,
                                        true, state);
                            }
                        }

                        // rescheduling to emit the current batch after expiring it if no further events arrive.
                        state.lastScheduledTime = siddhiQueryContext.getSiddhiAppContext().
                                getTimestampGenerator().currentTime() + schedulerTimeout;
                        scheduler.notifyAt(state.lastScheduledTime);
                    }
                    continue;
                } else if (currStreamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }

                long currentEventTime = (Long) timestampExpressionExecutor.execute(currStreamEvent);
                if (state.lastCurrentEventTime < currentEventTime) {
                    state.lastCurrentEventTime = currentEventTime;
                }

                if (currentEventTime < state.endTime) {
                    cloneAppend(streamEventCloner, currStreamEvent, state);
                } else {
                    if (state.flushed) {
                        appendToOutputChunk(streamEventCloner, complexEventChunks, state.lastCurrentEventTime,
                                false, state);
                        state.flushed = false;
                    } else {
                        flushToOutputChunk(streamEventCloner, complexEventChunks, state.lastCurrentEventTime,
                                false, state);
                    }
                    // update timestamp, call next processor
                    state.endTime = findEndTime(state.lastCurrentEventTime, state.startTime, timeToKeep);
                    cloneAppend(streamEventCloner, currStreamEvent, state);
                    // triggering the last batch expiration.
                    if (schedulerTimeout > 0) {
                        state.lastScheduledTime = siddhiQueryContext.getSiddhiAppContext().
                                getTimestampGenerator().currentTime() + schedulerTimeout;
                        scheduler.notifyAt(state.lastScheduledTime);
                    }
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> complexEventChunk : complexEventChunks) {
            nextProcessor.process(complexEventChunk);
        }
    }

    private void initTiming(StreamEvent firstStreamEvent, WindowState state) {
        // for window beginning, if window is empty, set lastSendTime to incomingChunk first.
        if (state.endTime < 0) {
            if (isStartTimeEnabled) {
                if (startTimeAsVariable == null) {
                    state.endTime = findEndTime((Long) timestampExpressionExecutor.execute(firstStreamEvent),
                            state.startTime, timeToKeep);
                } else {
                    state.startTime = (Long) startTimeAsVariable.execute(firstStreamEvent);
                    state.endTime = state.startTime + timeToKeep;
                }
            } else {
                state.startTime = (Long) timestampExpressionExecutor.execute(firstStreamEvent);
                state.endTime = state.startTime + timeToKeep;
            }
            if (schedulerTimeout > 0) {
                state.lastScheduledTime = siddhiQueryContext.getSiddhiAppContext().
                        getTimestampGenerator().currentTime() + schedulerTimeout;
                scheduler.notifyAt(state.lastScheduledTime);
            }
        }
    }

    private void flushToOutputChunk(StreamEventCloner streamEventCloner,
                                    List<ComplexEventChunk<StreamEvent>> complexEventChunks,
                                    long currentTime, boolean preserveCurrentEvents, WindowState state) {

        ComplexEventChunk<StreamEvent> newEventChunk = new ComplexEventChunk<>();
        if (outputExpectsExpiredEvents) {
            if (state.expiredEventChunk.getFirst() != null) {
                // mark the timestamp for the expiredType event
                state.expiredEventChunk.reset();
                while (state.expiredEventChunk.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventChunk.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                // add expired event to newEventChunk.
                newEventChunk.add(state.expiredEventChunk.getFirst());
            }
        }
        if (state.expiredEventChunk != null) {
            state.expiredEventChunk.clear();
        }

        if (state.currentEventChunk.getFirst() != null) {

            // add reset event in front of current events
            state.resetEvent.setTimestamp(currentTime);
            newEventChunk.add(state.resetEvent);
            state.resetEvent = null;

            // move to expired events
            if (preserveCurrentEvents || state.expiredEventChunk != null) {
                state.currentEventChunk.reset();
                while (state.currentEventChunk.hasNext()) {
                    StreamEvent currentEvent = state.currentEventChunk.next();
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    state.expiredEventChunk.add(toExpireEvent);
                }
            }

            // add current event chunk to next processor
            newEventChunk.add(state.currentEventChunk.getFirst());
        }
        state.currentEventChunk.clear();

        if (newEventChunk.getFirst() != null) {
            complexEventChunks.add(newEventChunk);
        }
    }

    private void appendToOutputChunk(StreamEventCloner streamEventCloner,
                                     List<ComplexEventChunk<StreamEvent>> complexEventChunks,
                                     long currentTime, boolean preserveCurrentEvents, WindowState state) {
        ComplexEventChunk<StreamEvent> newEventChunk = new ComplexEventChunk<>();
        ComplexEventChunk<StreamEvent> sentEventChunk = new ComplexEventChunk<>();
        if (state.currentEventChunk.getFirst() != null) {
            if (state.expiredEventChunk != null && state.expiredEventChunk.getFirst() != null) {
                // mark the timestamp for the expiredType event
                state.expiredEventChunk.reset();
                while (state.expiredEventChunk.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventChunk.next();

                    if (outputExpectsExpiredEvents) {
                        // add expired event to newEventChunk.
                        StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(expiredEvent);
                        toExpireEvent.setTimestamp(currentTime);
                        newEventChunk.add(toExpireEvent);
                    }

                    StreamEvent toSendEvent = streamEventCloner.copyStreamEvent(expiredEvent);
                    toSendEvent.setType(ComplexEvent.Type.CURRENT);
                    sentEventChunk.add(toSendEvent);
                }
            }

            // add reset event in front of current events
            StreamEvent toResetEvent = streamEventCloner.copyStreamEvent(state.resetEvent);
            toResetEvent.setTimestamp(currentTime);
            newEventChunk.add(toResetEvent);

            //add old events
            newEventChunk.add(sentEventChunk.getFirst());

            // move to expired events
            if (preserveCurrentEvents || state.expiredEventChunk != null) {
                state.currentEventChunk.reset();
                while (state.currentEventChunk.hasNext()) {
                    StreamEvent currentEvent = state.currentEventChunk.next();
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    state.expiredEventChunk.add(toExpireEvent);
                }
            }

            // add current event chunk to next processor
            newEventChunk.add(state.currentEventChunk.getFirst());

        }
        state.currentEventChunk.clear();

        if (newEventChunk.getFirst() != null) {
            complexEventChunks.add(newEventChunk);
        }
    }

    private long findEndTime(long currentTime, long startTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }

    private void cloneAppend(StreamEventCloner streamEventCloner, StreamEvent currStreamEvent, WindowState state) {
        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
        if (replaceTimestampWithBatchEndTime) {
            clonedStreamEvent.setAttribute(state.endTime, timestampExpressionExecutor.getPosition());
        }
        state.currentEventChunk.add(clonedStreamEvent);
        if (state.resetEvent == null) {
            state.resetEvent = streamEventCloner.copyStreamEvent(currStreamEvent);
            state.resetEvent.setType(ComplexEvent.Type.RESET);
        }
    }

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
        return OperatorParser.constructOperator(state.expiredEventChunk, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventChunk, streamEventCloner);

    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    class WindowState extends State {

        private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>();
        private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
        private StreamEvent resetEvent = null;
        private long endTime = -1;
        private long startTime = 0;
        private long lastScheduledTime;
        private long lastCurrentEventTime;
        private boolean flushed = false;


        public WindowState(boolean outputExpectsExpiredEvents, long schedulerTimeout, long startTime) {
            this.startTime = startTime;
            if (outputExpectsExpiredEvents || findToBeExecuted) {
                this.expiredEventChunk = new ComplexEventChunk<>();
            }
            if (schedulerTimeout > 0) {
                if (expiredEventChunk == null) {
                    this.expiredEventChunk = new ComplexEventChunk<>();
                }
            }
        }


        @Override
        public boolean canDestroy() {
            return currentEventChunk.getFirst() == null &&
                    (expiredEventChunk == null || expiredEventChunk.getFirst() == null) &&
                    resetEvent == null && flushed;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("StartTime", startTime);
            state.put("EndTime", endTime);
            state.put("LastScheduledTime", lastScheduledTime);
            state.put("LastCurrentEventTime", lastCurrentEventTime);
            state.put("CurrentEventChunk", currentEventChunk.getFirst());
            state.put("ExpiredEventChunk", expiredEventChunk != null ? expiredEventChunk.getFirst() : null);
            state.put("ResetEvent", resetEvent);
            state.put("Flushed", flushed);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            startTime = (long) state.get("StartTime");
            endTime = (long) state.get("EndTime");
            lastScheduledTime = (long) state.get("LastScheduledTime");
            lastCurrentEventTime = (long) state.get("LastCurrentEventTime");
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state.get("CurrentEventChunk"));
            if (expiredEventChunk != null) {
                expiredEventChunk.clear();
                expiredEventChunk.add((StreamEvent) state.get("ExpiredEventChunk"));
            } else {
                if (outputExpectsExpiredEvents || findToBeExecuted) {
                    expiredEventChunk = new ComplexEventChunk<>();
                }
                if (schedulerTimeout > 0) {
                    expiredEventChunk = new ComplexEventChunk<>();
                }
            }
            resetEvent = (StreamEvent) state.get("ResetEvent");
            flushed = (boolean) state.get("Flushed");
        }
    }
}

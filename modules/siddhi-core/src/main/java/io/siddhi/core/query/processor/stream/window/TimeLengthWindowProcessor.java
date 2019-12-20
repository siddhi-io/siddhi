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
 * Implementation of {@link WindowProcessor} which represent a Window operating based on pre-defined length.
 */
@Extension(
        name = "timeLength",
        namespace = "",
        description = "A sliding time window that, at a given time holds the last window.length events that arrived " +
                "during last window.time period, and gets updated for every event arrival and expiry.",
        parameters = {
                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
                @Parameter(name = "window.length",
                        description = "The number of events that should be be included in a sliding length window..",
                        type = {DataType.INT})
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"window.time", "window.length"})
        },
        examples = @Example(
                syntax = "define stream cseEventStream (symbol string, price float, volume int);\n" +
                        "define window cseEventWindow (symbol string, price float, volume int) " +
                        "timeLength(2 sec, 10);\n" +
                        "@info(name = 'query0')\n" +
                        "from cseEventStream\n" +
                        "insert into cseEventWindow;\n" +
                        "@info(name = 'query1')\n" +
                        "from cseEventWindow select symbol, price, volume\n" +
                        "insert all events into outputStream;",
                description = "window.timeLength(2 sec, 10) holds the last 10 events that arrived during " +
                        "last 2 seconds and gets updated for every event arrival and expiry."
        )
)
public class TimeLengthWindowProcessor extends SlidingFindableWindowProcessor<TimeLengthWindowProcessor.WindowState>
        implements SchedulingProcessor {

    private long timeInMilliSeconds;
    private int length;
    private Scheduler scheduler;
    private SiddhiQueryContext siddhiQueryContext;

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
                                SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        if (attributeExpressionExecutors.length == 2) {
            length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue();

                } else if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException("TimeLength window's first parameter attribute should " +
                            "be either int or long, but found " + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("TimeLength window should have constant parameter " +
                        "attributes but found a dynamic attribute " + attributeExpressionExecutors[0].getClass()
                        .getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException("TimeLength window should only have two parameters (<int> " +
                    "windowTime,<int> windowLength), but found " + attributeExpressionExecutors.length + " input " +
                    "attributes");
        }
        return () -> new WindowState(streamEventClonerHolder);

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {

        synchronized (state) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();

            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                state.expiredEventQueue.reset();
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        state.expiredEventQueue.remove();
                        state.count--;
                        expiredEvent.setTimestamp(currentTime);
                        streamEventChunk.insertBeforeCurrent(expiredEvent);
                    } else {
                        break;
                    }
                }

                state.expiredEventQueue.reset();

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    clonedEvent.setType(StreamEvent.Type.EXPIRED);
                    if (state.count < length) {
                        state.count++;
                        state.expiredEventQueue.add(clonedEvent);
                    } else {
                        StreamEvent firstEvent = state.expiredEventQueue.poll();
                        if (firstEvent != null) {
                            firstEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(firstEvent);
                            state.expiredEventQueue.add(clonedEvent);
                        }
                    }
                    scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                } else {
                    streamEventChunk.remove();
                }
            }
        }
        streamEventChunk.reset();
        if (streamEventChunk.hasNext()) {
            nextProcessor.process(streamEventChunk);
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

    class WindowState extends State {
        private SnapshotableStreamEventQueue expiredEventQueue;
        private int count = 0;

        WindowState(StreamEventClonerHolder streamEventClonerHolder) {
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            state.put("Count", count);
            return state;
        }

        public void restore(Map<String, Object> state) {
            expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            count = (Integer) state.get("ExpiredEventQueue");
        }

        @Override
        public boolean canDestroy() {
            return expiredEventQueue.getFirst() == null && count == 0;
        }
    }
}


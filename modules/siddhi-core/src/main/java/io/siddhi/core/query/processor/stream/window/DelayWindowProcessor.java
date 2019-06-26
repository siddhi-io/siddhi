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
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on delay time.
 */
@Extension(
        name = "delay",
        namespace = "",
        description = "A delay window holds events for a specific time period that is regarded as a" +
                " delay period before processing them.",
        parameters = {
                @Parameter(name = "window.delay",
                        description = "The time period (specified in sec, min, ms) for which " +
                                " the window should delay the events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME})
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"window.delay"})
        },
        examples = {
                @Example(
                        syntax = "define window delayWindow(symbol string, volume int) delay(1 hour);\n" +
                                "define stream PurchaseStream(symbol string, volume int);\n" +
                                "define stream DeliveryStream(symbol string);\n" +
                                "define stream OutputStream(symbol string);\n" +
                                "\n" +
                                "@info(name='query1') \n" +
                                "from PurchaseStream\n" +
                                "select symbol, volume\n" +
                                "insert into delayWindow;\n" +
                                "\n" +
                                "@info(name='query2') \n" +
                                "from delayWindow join DeliveryStream\n" +
                                "on delayWindow.symbol == DeliveryStream.symbol\n" +
                                "select delayWindow.symbol\n" +
                                "insert into OutputStream;",
                        description = "In this example, purchase events that arrive in the 'PurchaseStream' stream" +
                                " are directed to a delay window. At any given time, this delay window holds purchase" +
                                " events that have arrived within the last hour. These purchase events in the " +
                                "window are matched by the 'symbol' attribute, with delivery events that arrive in " +
                                "the 'DeliveryStream' stream. This monitors whether the delivery of products is done" +
                                " with a minimum delay of one hour after the purchase."
                )
        }
)

public class DelayWindowProcessor extends TimeWindowProcessor {

    private long delayInMilliSeconds;
    private SiddhiQueryContext siddhiQueryContext;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        if (attributeExpressionExecutors.length == 1) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT ||
                        attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                    delayInMilliSeconds = Long.parseLong(((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue().toString());
                } else {
                    throw new SiddhiAppValidationException("Delay window's parameter attribute should be either " +
                            "int or long, but found " + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Delay window should have constant parameter attribute but " +
                        "found a dynamic attribute " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException("Delay window should only have one parameter (<int|long|time> " +
                    "delayTime), but found " + attributeExpressionExecutors.length + " input attributes");
        }
        return () -> new DelayedWindowState(streamEventClonerHolder);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState windowState) {
        DelayedWindowState state = ((DelayedWindowState) windowState);
        synchronized (state) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();

                state.delayedEventQueue.reset();
                while (state.delayedEventQueue.hasNext()) {
                    StreamEvent delayedEvent = state.delayedEventQueue.next();
                    long timeDiff = delayedEvent.getTimestamp() - currentTime + delayInMilliSeconds;
                    if (timeDiff <= 0) {
                        state.delayedEventQueue.remove();
                        //insert delayed event before the current event to stream chunk
                        delayedEvent.setTimestamp(currentTime);
                        streamEventChunk.insertBeforeCurrent(delayedEvent);
                    } else {
                        break;
                    }
                }

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    state.delayedEventQueue.add(streamEvent);
                    if (state.lastTimestamp < streamEvent.getTimestamp()) {
                        getScheduler().notifyAt(streamEvent.getTimestamp() + delayInMilliSeconds);
                        state.lastTimestamp = streamEvent.getTimestamp();
                    }
                }
                //current events are not processed, so remove the current event from the stream chunk
                streamEventChunk.remove();
            }
            state.delayedEventQueue.reset();
        }
        //only pass to next processor if there are any events in the stream chunk
        if (streamEventChunk.getFirst() != null) {
            nextProcessor.process(streamEventChunk);
        }
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, ((DelayedWindowState) state).delayedEventQueue,
                streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(((DelayedWindowState) state).delayedEventQueue,
                condition, matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    class DelayedWindowState extends WindowState {
        private SnapshotableStreamEventQueue delayedEventQueue;
        private volatile long lastTimestamp = Long.MIN_VALUE;

        DelayedWindowState(StreamEventClonerHolder streamEventClonerHolder) {
            super(streamEventClonerHolder);
            this.delayedEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("DelayedEventQueue", delayedEventQueue.getSnapshot());
            state.put("LastTimestamp", lastTimestamp);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            delayedEventQueue.restore((SnapshotStateList) state.get("DelayedEventQueue"));
            lastTimestamp = (long) state.get("LastTimestamp");
        }

        @Override
        public boolean canDestroy() {
            return delayedEventQueue.getFirst() == null;
        }
    }
}

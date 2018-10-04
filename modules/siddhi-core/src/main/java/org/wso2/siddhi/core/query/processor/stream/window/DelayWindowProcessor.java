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
package org.wso2.siddhi.core.query.processor.stream.window;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.core.util.snapshot.state.SnapshotStateList;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on delay time.
 */
@Extension(
        name = "delay",
        namespace = "",
        description = "A delay window holds events that arrived " +
                "for a given delay time period before processing them.",
        parameters = {
                @Parameter(name = "window.delay",
                        description = "The time period(sec, min, ms) for which " +
                                " the window should delay the events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME})
        },
        examples = {
                @Example(
                        syntax = "define window delayWindow(symbol string, volume int) delay(20);\n" +
                                "define stream inputStream(symbol string, volume int);\n" +
                                "define stream deliveryStream(symbol string);\n" +
                                "define stream outputStream(symbol string);\n" +
                                "\n" +
                                "@info(name='query1') \n" +
                                "from inputStream\n" +
                                "select symbol, volume\n" +
                                "insert into delayWindow;\n" +
                                "\n" +
                                "@info(name='query2') \n" +
                                "from delayWindow join deliveryStream\n" +
                                "on delayWindow.symbol == deliveryStream.symbol\n" +
                                "select delayWindow.symbol\n" +
                                "insert into outputStream;",
                        description = "This will delay the events from inputStream for 20 milliseconds " +
                                "and match them with events arrived from deliveryStream  "
                )
        }
)

public class DelayWindowProcessor extends TimeWindowProcessor {

    private long delayInMilliSeconds;
    private SiddhiAppContext siddhiAppContext;
    private SnapshotableStreamEventQueue delayedEventQueue;
    private volatile long lastTimestamp = Long.MIN_VALUE;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, boolean
            outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.delayedEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
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
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {

            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();

                delayedEventQueue.reset();
                while (delayedEventQueue.hasNext()) {
                    StreamEvent delayedEvent = delayedEventQueue.next();
                    //check if the event has delayed expected time period
                    if (currentTime >= delayedEvent.getTimestamp() + delayInMilliSeconds) {
                        delayedEventQueue.remove();
                        //insert delayed event before the current event to stream chunk
                        streamEventChunk.insertBeforeCurrent(delayedEvent);
                    } else {
                        break;
                    }
                }

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    this.delayedEventQueue.add(streamEvent);

                    if (lastTimestamp < streamEvent.getTimestamp()) {
                        getScheduler().notifyAt(streamEvent.getTimestamp() + delayInMilliSeconds);
                        lastTimestamp = streamEvent.getTimestamp();
                    }
                }
                //current events are not processed, so remove the current event from the stream chunk
                streamEventChunk.remove();
            }
            delayedEventQueue.reset();
        }
        //only pass to next processor if there are any events in the stream chunk
        if (streamEventChunk.getFirst() != null) {
            nextProcessor.process(streamEventChunk);
        }
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return ((Operator) compiledCondition).find(matchingEvent, delayedEventQueue, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, String queryName) {
        return OperatorParser.constructOperator(delayedEventQueue, condition, matchingMetaInfoHolder,
                siddhiAppContext, variableExpressionExecutors, tableMap, this.queryName);
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("DelayedEventQueue", delayedEventQueue.getSnapshot());
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        delayedEventQueue.restore((SnapshotStateList) state.get("DelayedEventQueue"));
    }
}

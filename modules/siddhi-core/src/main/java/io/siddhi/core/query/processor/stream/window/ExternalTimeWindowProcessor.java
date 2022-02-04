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
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on external time.
 */
@Extension(
        name = "externalTime",
        namespace = "",
        description = "A sliding time window based on external time. It holds events that arrived during the last " +
                "windowTime period from the external timestamp, and gets updated on every monotonically " +
                "increasing timestamp.",
        parameters = {
                @Parameter(name = "timestamp",
                        description = "The time which the window determines as current time and will act upon. " +
                                "The value of this parameter should be monotonically increasing.",
                        type = {DataType.LONG},
                        dynamic = true),

                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME}),
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"timestamp", "window.time"})
        },
        examples = @Example(
                syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                        "externalTime(eventTime, 20 sec) output expired events;\n\n" +
                        "@info(name = 'query0')\n" +
                        "from cseEventStream\n" +
                        "insert into cseEventWindow;\n\n" +
                        "@info(name = 'query1')\n" +
                        "from cseEventWindow\n" +
                        "select symbol, sum(price) as price\n" +
                        "insert expired events into outputStream ;",
                description = "processing events arrived within the last 20 seconds " +
                        "from the eventTime and output expired events."
        )
)
public class ExternalTimeWindowProcessor
        extends SlidingFindableWindowProcessor<ExternalTimeWindowProcessor.WindowState> {
    private static final Logger log = LogManager.getLogger(ExternalTimeWindowProcessor.class);
    private long timeToKeep;
    private VariableExpressionExecutor timeStampVariableExpressionExecutor;

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                timeToKeep = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            } else {
                timeToKeep = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue()));
            }
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException("ExternalTime window's 1st parameter timeStamp should be a" +
                        " type long stream attribute but found " + attributeExpressionExecutors[0].getClass());
            }
            timeStampVariableExpressionExecutor = ((VariableExpressionExecutor) attributeExpressionExecutors[0]);
            if (timeStampVariableExpressionExecutor.getReturnType() != Attribute.Type.LONG) {
                throw new SiddhiAppValidationException("ExternalTime window's 1st parameter timeStamp should be " +
                        "type long, but found " + timeStampVariableExpressionExecutor.getReturnType());
            }
        } else {
            throw new SiddhiAppValidationException("ExternalTime window should only have two parameter (<long> " +
                    "timeStamp, <int|long|time> windowTime), but found " + attributeExpressionExecutors.length + " " +
                    "input attributes");
        }
        return () -> new WindowState();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {
        synchronized (state) {
            while (streamEventChunk.hasNext()) {

                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = (Long) timeStampVariableExpressionExecutor.execute(streamEvent);

                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);

                // reset expiredEventQueue to make sure all of the expired events get removed,
                // otherwise lastReturned.next will always return null and here while check is always false
                state.expiredEventQueue.reset();
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    long expiredEventTime = (Long) timeStampVariableExpressionExecutor.execute(expiredEvent);
                    long timeDiff = expiredEventTime - currentTime + timeToKeep;
                    if (timeDiff <= 0) {
                        state.expiredEventQueue.remove();
                        expiredEvent.setTimestamp(currentTime);
                        streamEventChunk.insertBeforeCurrent(expiredEvent);
                    } else {
                        state.expiredEventQueue.reset();
                        break;
                    }
                }

                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    state.expiredEventQueue.add(clonedEvent);
                }
                state.expiredEventQueue.reset();
            }
        }
        nextProcessor.process(streamEventChunk);
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
        private SnapshotableStreamEventQueue expiredEventQueue =
                new SnapshotableStreamEventQueue(streamEventClonerHolder);

        @Override
        public boolean canDestroy() {
            return expiredEventQueue.getFirst() == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            expiredEventQueue.clear();
            expiredEventQueue.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
        }
    }
}

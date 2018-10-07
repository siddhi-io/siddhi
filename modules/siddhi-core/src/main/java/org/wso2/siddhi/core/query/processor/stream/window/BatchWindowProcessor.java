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
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

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
                "in batches.",
        parameters = {
                // TODO: 7/7/18 Add parameter batch.size.max
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
public class BatchWindowProcessor extends WindowProcessor implements FindableProcessor {

    private SnapshotableStreamEventQueue expiredEventQueue = null;
    private boolean outputExpectsExpiredEvents;
    private SiddhiAppContext siddhiAppContext;
    private StreamEvent resetEvent = null;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.siddhiAppContext = siddhiAppContext;
        if (outputExpectsExpiredEvents) {
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }
        if (attributeExpressionExecutors.length != 0) {
            throw new SiddhiAppValidationException(
                    "Batch window should not have any parameters, but found " + attributeExpressionExecutors.length +
                            " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            if (outputExpectsExpiredEvents) {
                if (expiredEventQueue.getFirst() != null) {
                    while (expiredEventQueue.hasNext()) {
                        expiredEventQueue.next().setTimestamp(currentTime);
                    }
                    outputStreamEventChunk.add(expiredEventQueue.getFirst());
                }
                expiredEventQueue.clear();
            }
            if (resetEvent != null) {
                outputStreamEventChunk.add(resetEvent);
            }

            //check whether the streamEventChunk has next event before add into output stream event chunk
            if (streamEventChunk.hasNext()) {
                if (outputExpectsExpiredEvents) {
                    do {
                        StreamEvent streamEvent = streamEventChunk.next();
                        StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        clonedStreamEvent.setType(StreamEvent.Type.EXPIRED);
                        expiredEventQueue.add(clonedStreamEvent);
                    } while (streamEventChunk.hasNext());
                }
                resetEvent = streamEventCloner.copyStreamEvent(streamEventChunk.getFirst());
                resetEvent.setType(ComplexEvent.Type.RESET);
                outputStreamEventChunk.add(streamEventChunk.getFirst());
            }
        }
        nextProcessor.process(outputStreamEventChunk);
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
            if (outputExpectsExpiredEvents) {
                state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            }
            state.put("ResetEvent", resetEvent);
        }
        return state;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> state) {
        if (outputExpectsExpiredEvents) {
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
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, String queryName) {
        if (expiredEventQueue == null) {
            expiredEventQueue = new SnapshotableStreamEventQueue(streamEventClonerHolder);
        }
        return OperatorParser.constructOperator(expiredEventQueue, condition, matchingMetaInfoHolder,
                siddhiAppContext, variableExpressionExecutors, tableMap,
                this.queryName);
    }
}

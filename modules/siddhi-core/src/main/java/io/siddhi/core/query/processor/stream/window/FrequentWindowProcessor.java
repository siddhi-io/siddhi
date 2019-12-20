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
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on frequency of incoming events.
 * Implementation uses a counting algorithm based on Misra-Gries counting algorithm
 */
@Extension(
        name = "frequent",
        namespace = "",
        description = "This window returns the latest events with the most frequently occurred value for " +
                "a given attribute(s). Frequency calculation for this window processor is based on " +
                "Misra-Gries counting algorithm.",
        deprecated = true,
        parameters = {
                @Parameter(name = "event.count",
                        description = "The number of most frequent events to be emitted to the stream.",
                        type = {DataType.INT}),
                @Parameter(name = "attribute",
                        description = "The attributes to group the events. If no attributes are given, " +
                                "the concatenation of all the attributes of the event is considered.",
                        type = {DataType.STRING},
                        optional = true,
                        dynamic = true,
                        defaultValue = "The concatenation of all the attributes of the event is considered.")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"event.count"}),
                @ParameterOverload(parameterNames = {"event.count", "attribute"})
        },
        examples = {
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from purchase[price >= 30]#window.frequent(2)\n" +
                                "select cardNo, price\n" +
                                "insert all events into PotentialFraud;",
                        description = "This will returns the 2 most frequent events."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from purchase[price >= 30]#window.frequent(2, cardNo)\n" +
                                "select cardNo, price\n" +
                                "insert all events into PotentialFraud;",
                        description = "This will returns the 2 latest events with the most frequently appeared " +
                                "card numbers."
                )
        }
)
@Deprecated
public class FrequentWindowProcessor extends SlidingFindableWindowProcessor<FrequentWindowProcessor.WindowState> {
    private VariableExpressionExecutor[] variableExpressionExecutors;

    private int mostFrequentCount;

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader,
                                             SiddhiQueryContext siddhiQueryContext) {
        mostFrequentCount = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                attributeExpressionExecutors[0]).getValue()));
        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
        for (int i = 1; i < attributeExpressionExecutors.length; i++) {
            variableExpressionExecutors[i - 1] = (VariableExpressionExecutor) attributeExpressionExecutors[i];
        }
        return () -> new WindowState();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {

        synchronized (this) {
            StreamEvent streamEvent = streamEventChunk.getFirst();
            streamEventChunk.clear();
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEvent != null) {
                StreamEvent next = streamEvent.getNext();
                streamEvent.setNext(null);

                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);

                String key = generateKey(streamEvent);
                StreamEvent oldEvent = state.map.put(key, clonedEvent);
                if (oldEvent != null) {
                    state.countMap.put(key, state.countMap.get(key) + 1);
                    streamEventChunk.add(streamEvent);
                } else {
                    //  This is a new event
                    if (state.map.size() > mostFrequentCount) {
                        List<String> keys = new ArrayList<String>(state.countMap.keySet());
                        for (int i = 0; i < mostFrequentCount; i++) {
                            int count = state.countMap.get(keys.get(i)) - 1;
                            if (count == 0) {
                                state.countMap.remove(keys.get(i));
                                StreamEvent expiredEvent = state.map.remove(keys.get(i));
                                expiredEvent.setTimestamp(currentTime);
                                streamEventChunk.add(expiredEvent);
                            } else {
                                state.countMap.put(keys.get(i), count);
                            }
                        }
                        // now we have tried to remove one for newly added item
                        if (state.map.size() > mostFrequentCount) {
                            //nothing happend by the attempt to remove one from the
                            // map so we are ignoring this event
                            state.map.remove(key);
                            // Here we do nothing just drop the message
                        } else {
                            // we got some space, event is already there in map object
                            // we just have to add it to the countMap
                            state.countMap.put(key, 1);
                            streamEventChunk.add(streamEvent);
                        }
                    } else {
                        state.countMap.put(generateKey(streamEvent), 1);
                        streamEventChunk.add(streamEvent);
                    }
                }
                streamEvent = next;
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

    private String generateKey(StreamEvent event) {      // for performance reason if its all attribute we don't do
        // the attribute list check
        StringBuilder stringBuilder = new StringBuilder();
        if (variableExpressionExecutors.length == 0) {
            for (Object data : event.getOutputData()) {
                stringBuilder.append(data);
            }
        } else {
            for (VariableExpressionExecutor executor : variableExpressionExecutors) {
                stringBuilder.append(event.getAttribute(executor.getPosition()));
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(state.map.values(), condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.map.values(), streamEventCloner);
    }

    class WindowState extends State {
        private ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
        private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();

        @Override
        public boolean canDestroy() {
            return countMap.isEmpty() && map.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("CountMap", countMap);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            countMap = (ConcurrentHashMap<String, Integer>) state.get("CountMap");
        }
    }
}

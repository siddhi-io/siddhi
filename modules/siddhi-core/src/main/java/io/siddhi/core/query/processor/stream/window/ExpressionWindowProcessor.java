/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.config.SiddhiOnDemandQueryContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.compiler.SiddhiCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on a expression.
 */
@Extension(
        name = "expression",
        namespace = "",
        description = "A sliding window that dynamically shrink and grow based on the `expression`, " +
                "it holds events that satisfies the given `expression`, when they aren't, " +
                "they are evaluated from the `first` (oldest) to the `last` (latest/current) and expired " +
                "from the oldest until the `expression` is satisfied.\n" +
                "**Note**: All the events in window are reevaluated only when the given `expression` is changed.",
        parameters = {
                @Parameter(name = "expression",
                        description = "The expression to retain events.",
                        type = {DataType.STRING},
                        dynamic = true
                )
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"expression"})
        },
        examples = {
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expression('count()<=20')\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "This will retain last 20 events in a sliding manner."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expression(\n" +
                                "       'sum(price) < 100 and eventTimestamp(last) - eventTimestamp(first) < 3000')\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "This will retain the latest events having their sum(price) < 100, " +
                                "and the `last` and `first` events are within 3 second difference."
                )}
)
public class ExpressionWindowProcessor extends SlidingFindableWindowProcessor<ExpressionWindowProcessor.WindowState> {

    private ExpressionExecutor expressionExecutor;
    private List<VariableExpressionExecutor> variableExpressionExecutors;
    private boolean init = false;
    private ExpressionExecutor expressionStringExecutor = null;
    private String expressionString = null;

    @Override
    protected StateFactory init(MetaStreamEvent metaStreamEvent,
                                AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, StreamEventClonerHolder streamEventClonerHolder,
                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            expressionString = (String) ((ConstantExpressionExecutor)
                    attributeExpressionExecutors[0]).getValue();
            constructExpression(metaStreamEvent, siddhiQueryContext);
        } else {
            for (Attribute attribute : inputDefinition.getAttributeList()) {
                metaStreamEvent.addData(attribute);
            }
            expressionStringExecutor = attributeExpressionExecutors[0];
        }
        return () -> new WindowState();
    }

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        //will not be called.
        return null;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, WindowState state) {

        if (expressionStringExecutor == null && !init) {
            MetaStateEvent metaStateEvent = new MetaStateEvent(
                    new MetaStreamEvent[]{metaStreamEvent, metaStreamEvent, metaStreamEvent});
            QueryParserHelper.updateVariablePosition(metaStateEvent, variableExpressionExecutors);
            init = true;
        }
        synchronized (state) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (expressionStringExecutor != null) {
                    String expressionStringNew = (String) expressionStringExecutor.execute(streamEvent);
                    if (!expressionStringNew.equals(expressionString)) {
                        expressionString = expressionStringNew;
                        processAllExpiredEvents(streamEventChunk, state, currentTime);
                    }
                }
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);
                processStreamEvent(streamEventChunk, state, currentTime, clonedEvent);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    private MetaStateEvent constructExpression(MetaStreamEvent metaStreamEvent,
                                               SiddhiQueryContext siddhiQueryContext) {
        Expression expression = SiddhiCompiler.parseExpression(expressionString);
        MetaStreamEvent metaStreamEventFirst = new MetaStreamEventWrapper(metaStreamEvent);
        metaStreamEventFirst.setInputReferenceId("first");
        MetaStreamEvent metaStreamEventLast = new MetaStreamEventWrapper(metaStreamEvent);
        metaStreamEventLast.setInputReferenceId("last");
        MetaStateEvent metaStateEvent = new MetaStateEvent(
                new MetaStreamEvent[]{metaStreamEvent, metaStreamEventFirst, metaStreamEventLast});
        variableExpressionExecutors = new ArrayList<>();
        SiddhiQueryContext exprQueryContext = new SiddhiOnDemandQueryContext(
                siddhiQueryContext.getSiddhiAppContext(), siddhiQueryContext.getName(),
                expressionString);
        expressionExecutor = ExpressionParser.parseExpression(expression, metaStateEvent,
                0, new HashMap<>(), variableExpressionExecutors, false,
                0, ProcessingMode.SLIDE, true, exprQueryContext);
        if (expressionExecutor.getReturnType() != Attribute.Type.BOOL) {
            throw new SiddhiAppRuntimeException("Expression ('" + expressionString + "') does not return Bool");
        }
        return metaStateEvent;
    }

    private void processAllExpiredEvents(ComplexEventChunk<StreamEvent> streamEventChunk,
                                         WindowState state, long currentTime) {
        MetaStateEvent metaStateEvent = constructExpression(metaStreamEvent, siddhiQueryContext);
        QueryParserHelper.updateVariablePosition(metaStateEvent, variableExpressionExecutors);
        StreamEvent expiredEvent = state.expiredEventQueue.getFirst();
        state.expiredEventQueue.clear();
        while (expiredEvent != null) {
            StreamEvent aExpiredEvent = expiredEvent;
            expiredEvent = expiredEvent.getNext();
            aExpiredEvent.setNext(null);
            processStreamEvent(streamEventChunk, state, currentTime, aExpiredEvent);
        }
        state.expiredEventQueue.reset();
    }

    private void processStreamEvent(ComplexEventChunk<StreamEvent> streamEventChunk,
                                    WindowState state, long currentTime, StreamEvent streamEvent) {
        state.expiredEventQueue.add(streamEvent);
        StateEvent stateEventCurrent = new StateEvent(3, 0);
        stateEventCurrent.setEvent(0, streamEvent);
        stateEventCurrent.setEvent(1, state.expiredEventQueue.getFirst());
        stateEventCurrent.setEvent(2, streamEvent);
        if (!(Boolean) expressionExecutor.execute(stateEventCurrent)) {
            state.expiredEventQueue.reset();
            while (state.expiredEventQueue.hasNext()) {
                StreamEvent expiredEvent = state.expiredEventQueue.next();
                state.expiredEventQueue.remove();
                streamEventChunk.insertBeforeCurrent(expiredEvent);
                StateEvent stateEvent = new StateEvent(3, 0);
                stateEvent.setEvent(0, expiredEvent);
                StreamEvent firstEvent = state.expiredEventQueue.getFirst();
                if (firstEvent == null) {
                    firstEvent = expiredEvent;
                }
                stateEvent.setEvent(1, firstEvent);
                stateEvent.setEvent(2, streamEvent);
                stateEvent.setType(ComplexEvent.Type.EXPIRED);
                expiredEvent.setTimestamp(currentTime);
                if ((Boolean) expressionExecutor.execute(stateEvent)) {
                    break;
                }


            }
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
        //Do nothing
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

    class MetaStreamEventWrapper extends MetaStreamEvent {
        private MetaStreamEvent metaStreamEvent;
        private String inputReferenceId;

        public MetaStreamEventWrapper(MetaStreamEvent metaStreamEvent) {
            this.metaStreamEvent = metaStreamEvent;
        }

        @Override
        public List<Attribute> getBeforeWindowData() {
            return metaStreamEvent.getBeforeWindowData();
        }

        @Override
        public List<Attribute> getOnAfterWindowData() {
            return metaStreamEvent.getOnAfterWindowData();
        }

        @Override
        public List<Attribute> getOutputData() {
            return metaStreamEvent.getOutputData();
        }

        @Override
        public void initializeOnAfterWindowData() {
            metaStreamEvent.initializeOnAfterWindowData();
        }

        @Override
        public void initializeAfterWindowData() {
            metaStreamEvent.initializeAfterWindowData();
        }

        @Override
        public int addData(Attribute attribute) {
            return metaStreamEvent.addData(attribute);
        }

        @Override
        public void addOutputData(Attribute attribute) {
            metaStreamEvent.addOutputData(attribute);
        }

        @Override
        public void addOutputDataAllowingDuplicate(Attribute attribute) {
            metaStreamEvent.addOutputDataAllowingDuplicate(attribute);
        }

        @Override
        public List<AbstractDefinition> getInputDefinitions() {
            return metaStreamEvent.getInputDefinitions();
        }

        @Override
        public void addInputDefinition(AbstractDefinition inputDefinition) {
            metaStreamEvent.addInputDefinition(inputDefinition);
        }

        @Override
        public String getInputReferenceId() {
            return inputReferenceId;
        }

        @Override
        public void setInputReferenceId(String inputReferenceId) {
            this.inputReferenceId = inputReferenceId;
        }

        @Override
        public void setOutputDefinition(StreamDefinition streamDefinition) {
            metaStreamEvent.setOutputDefinition(streamDefinition);
        }

        @Override
        public StreamDefinition getOutputStreamDefinition() {
            return metaStreamEvent.getOutputStreamDefinition();
        }

        @Override
        public EventType getEventType() {
            return metaStreamEvent.getEventType();
        }

        @Override
        public void setEventType(EventType eventType) {
            metaStreamEvent.setEventType(eventType);
        }

        @Override
        public AbstractDefinition getLastInputDefinition() {
            return metaStreamEvent.getLastInputDefinition();
        }

        @Override
        public boolean isMultiValue() {
            return metaStreamEvent.isMultiValue();
        }

        @Override
        public void setMultiValue(boolean multiValue) {
            metaStreamEvent.setMultiValue(multiValue);
        }

        @Override
        public MetaStreamEvent clone() {
            return metaStreamEvent.clone();
        }
    }
}

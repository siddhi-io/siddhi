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
        name = "expressionBatch",
        namespace = "",
        description = "A batch window that dynamically shrink and grow based on the `expression`, " +
                "it holds events until the `expression` is satisfied, and expires " +
                "all when the `expression` is not satisfied." +
                "When a string is passed as the `expression` it is evaluated from the `first` (oldest) to the " +
                "`last` (latest/current).\n" +
                "**Note**: All the events in window are reevaluated only when the given `expression` is changed.",
        parameters = {
                @Parameter(name = "expression",
                        description = "The expression to retain events.",
                        type = {DataType.STRING, DataType.BOOL},
                        dynamic = true
                ),
                @Parameter(name = "include.triggering.event",
                        description = "Include the event triggered the expiry in to the current event batch.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false",
                        dynamic = true
                ),
                @Parameter(name = "stream.current.event",
                        description = "Let the window stream the current events out as and when they arrive" +
                                " to the window while expiring them in batches.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"
                )
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"expression"}),
                @ParameterOverload(parameterNames = {"expression", "include.triggering.event"}),
                @ParameterOverload(parameterNames = {"expression", "include.triggering.event", "stream.current.event"})
        },
        examples = {
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expressionBatch('count()<=20')\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "Retain and output 20 events at a time as batch."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expressionBatch(\n" +
                                "       'sum(price) < 100 and eventTimestamp(last) - eventTimestamp(first) < 3000')\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "Retain and output events having their sum(price) < 100, " +
                                "and the `last` and `first` events are within 3 second difference as a batch."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expressionBatch(\n" +
                                "       'last.symbol==first.symbol')\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "Output events as a batch when a new symbol type arrives."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expressionBatch(\n" +
                                "       'flush', true)\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "Output events as a batch when a flush attribute becomes `true`, the output " +
                                "batch will also contain the triggering event."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from StockEventWindow#window.expressionBatch(\n" +
                                "       'flush', false, true)\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert into OutputStream ;",
                        description = "Arriving events are emitted as soon as they are arrived, and the retained " +
                                "events are expired when flush attribute becomes `true`, and the output " +
                                "batch will not contain the triggering event."
                )}
)
public class ExpressionBatchWindowProcessor extends
        BatchingFindableWindowProcessor<ExpressionBatchWindowProcessor.WindowState> {

    private ExpressionExecutor expressionExecutor;
    private List<VariableExpressionExecutor> variableExpressionExecutors;
    private boolean init = false;
    private ExpressionExecutor expressionStringExecutor = null;
    private String expressionString = null;
    private boolean includeTriggeringEvent = false;
    private ExpressionExecutor includeTriggeringEventExecutor = null;
    private Boolean streamInputEvents = false;

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
        if (attributeExpressionExecutors.length > 1) {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                includeTriggeringEvent = (Boolean) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue();
            } else {
                includeTriggeringEventExecutor = attributeExpressionExecutors[1];
            }
            if (attributeExpressionExecutors.length > 2 &&
                    attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                streamInputEvents = (Boolean) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[2]).getValue();
            }
        }
        return () -> new WindowState();
    }

    @Override
    protected StateFactory<WindowState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                             ConfigReader configReader,
                                             StreamEventClonerHolder streamEventClonerHolder,
                                             boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
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
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<>();
        synchronized (state) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEventChunk.remove();
                if (expressionStringExecutor != null) {
                    String expressionStringNew = (String) expressionStringExecutor.execute(streamEvent);
                    if (!expressionStringNew.equals(expressionString)) {
                        expressionString = expressionStringNew;
                        processAllExpiredEvents(streamEventCloner, state, currentTime, streamEventChunks);
                    }
                }
                if (!streamInputEvents) {
                    processStreamEvent(state, currentTime, streamEvent, streamEventCloner, streamEventChunks);
                } else {
                    processStreamEventAsStream(state, currentTime, streamEvent, streamEventCloner, streamEventChunks,
                            true);
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
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

    private void processAllExpiredEvents(StreamEventCloner streamEventCloner, WindowState state,
                                         long currentTime, List<ComplexEventChunk<StreamEvent>> streamEventChunks) {
        MetaStateEvent metaStateEvent = constructExpression(metaStreamEvent, siddhiQueryContext);
        QueryParserHelper.updateVariablePosition(metaStateEvent, variableExpressionExecutors);
        if (!streamInputEvents) {
            StreamEvent expiredEvent = state.currentEventQueue.getFirst();
            state.currentEventQueue.clear();
            while (expiredEvent != null) {
                StreamEvent aExpiredEvent = expiredEvent;
                expiredEvent = expiredEvent.getNext();
                aExpiredEvent.setNext(null);
                processStreamEvent(state, currentTime, aExpiredEvent, streamEventCloner, streamEventChunks);
            }
            state.currentEventQueue.reset();
        } else {
            StreamEvent expiredEvent = state.expiredEventQueue.getFirst();
            state.expiredEventQueue.clear();
            while (expiredEvent != null) {
                StreamEvent aExpiredEvent = expiredEvent;
                expiredEvent = expiredEvent.getNext();
                aExpiredEvent.setNext(null);
                processStreamEventAsStream(state, currentTime, aExpiredEvent, streamEventCloner, streamEventChunks,
                        false);
            }
            state.expiredEventQueue.reset();
        }
    }

    private void processStreamEvent(WindowState state, long currentTime, StreamEvent streamEvent,
                                    StreamEventCloner streamEventCloner,
                                    List<ComplexEventChunk<StreamEvent>> streamEventChunks) {
        StateEvent stateEventCurrent = new StateEvent(3, 0);
        stateEventCurrent.setEvent(0, streamEvent);
        if (state.currentEventQueue.getFirst() != null) {
            stateEventCurrent.setEvent(1, state.currentEventQueue.getFirst());
        } else {
            stateEventCurrent.setEvent(1, streamEvent);
        }
        stateEventCurrent.setEvent(2, streamEvent);
        if (!(Boolean) expressionExecutor.execute(stateEventCurrent)) {
            stateEventCurrent.setType(ComplexEvent.Type.RESET);
            expressionExecutor.execute(stateEventCurrent);
            stateEventCurrent.setType(ComplexEvent.Type.CURRENT);
            expressionExecutor.execute(stateEventCurrent);
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<>();
            state.expiredEventQueue.reset();
            if (state.expiredEventQueue.getFirst() != null) {
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(state.expiredEventQueue.getFirst());
                state.expiredEventQueue.clear();
            }
            if (state.currentEventQueue.getFirst() != null) {
                while (state.currentEventQueue.hasNext()) {
                    StreamEvent currentEvent = state.currentEventQueue.next();
                    currentEvent.setTimestamp(currentTime);
                    StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                    state.expiredEventQueue.add(expiredEvent);
                }
                outputStreamEventChunk.add(state.currentEventQueue.getFirst());
                state.currentEventQueue.clear();
                if (includeTriggeringEventExecutor != null &&
                        (Boolean) includeTriggeringEventExecutor.execute(streamEvent) ||
                        includeTriggeringEvent) {
                    outputStreamEventChunk.add(streamEvent);
                    StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    expiredEvent.setTimestamp(currentTime);
                    expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                    state.expiredEventQueue.add(expiredEvent);
                } else {
                    state.currentEventQueue.add(streamEvent);
                }
            } else {
                StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
                expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                expiredEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(expiredEvent);
                streamEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(streamEvent);
            }
            streamEventChunks.add(outputStreamEventChunk);
        } else {
            state.currentEventQueue.add(streamEvent);
        }
    }

    private void processStreamEventAsStream(WindowState state, long currentTime, StreamEvent streamEvent,
                                            StreamEventCloner streamEventCloner,
                                            List<ComplexEventChunk<StreamEvent>> streamEventChunks, boolean output) {
        StateEvent stateEventCurrent = new StateEvent(3, 0);
        stateEventCurrent.setEvent(0, streamEvent);
        if (state.expiredEventQueue.getFirst() != null) {
            stateEventCurrent.setEvent(1, state.expiredEventQueue.getFirst());
        } else {
            stateEventCurrent.setEvent(1, streamEvent);
        }
        stateEventCurrent.setEvent(2, streamEvent);
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<>();
        if (output && state.currentEventQueue.getFirst() != null) {
            outputStreamEventChunk.add(state.currentEventQueue.getFirst());
            state.currentEventQueue.clear();
        }
        if (!(Boolean) expressionExecutor.execute(stateEventCurrent)) {
            stateEventCurrent.setType(ComplexEvent.Type.RESET);
            expressionExecutor.execute(stateEventCurrent);
            stateEventCurrent.setType(ComplexEvent.Type.CURRENT);
            expressionExecutor.execute(stateEventCurrent);
            state.expiredEventQueue.reset();
            if (state.expiredEventQueue.getFirst() != null) {
                while (state.expiredEventQueue.hasNext()) {
                    StreamEvent expiredEvent = state.expiredEventQueue.next();
                    expiredEvent.setTimestamp(currentTime);
                }
                StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
                expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                if (includeTriggeringEventExecutor != null &&
                        (Boolean) includeTriggeringEventExecutor.execute(streamEvent) ||
                        includeTriggeringEvent) {
                    outputStreamEventChunk.add(streamEvent);
                    if (output) {
                        outputStreamEventChunk.add(state.expiredEventQueue.getFirst());
                        expiredEvent.setTimestamp(currentTime);
                        outputStreamEventChunk.add(expiredEvent);
                    } else {
                        state.currentEventQueue.add(state.expiredEventQueue.getFirst());
                        state.currentEventQueue.add(expiredEvent);
                    }
                    state.expiredEventQueue.clear();
                } else {
                    if (output) {
                        outputStreamEventChunk.add(state.expiredEventQueue.getFirst());
                    } else {
                        state.currentEventQueue.add(state.expiredEventQueue.getFirst());
                    }
                    outputStreamEventChunk.add(streamEvent);
                    state.expiredEventQueue.clear();
                    state.expiredEventQueue.add(expiredEvent);
                }

            } else if (output) {
                StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
                expiredEvent.setType(ComplexEvent.Type.EXPIRED);
                expiredEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(expiredEvent);
                streamEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(streamEvent);
            } else {
                state.currentEventQueue.add(streamEvent);
            }
            if (output) {
                streamEventChunks.add(outputStreamEventChunk);
            }
        } else {
            StreamEvent expiredEvent = streamEventCloner.copyStreamEvent(streamEvent);
            expiredEvent.setType(ComplexEvent.Type.EXPIRED);
            state.expiredEventQueue.add(expiredEvent);
            if (output) {
                outputStreamEventChunk.add(streamEvent);
                streamEventChunks.add(outputStreamEventChunk);
            }
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, WindowState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(state.currentEventQueue, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, WindowState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.currentEventQueue, streamEventCloner);

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

        private SnapshotableStreamEventQueue currentEventQueue =
                new SnapshotableStreamEventQueue(streamEventClonerHolder);
        private SnapshotableStreamEventQueue expiredEventQueue =
                new SnapshotableStreamEventQueue(streamEventClonerHolder);

        @Override
        public boolean canDestroy() {
            return currentEventQueue.getFirst() == null && expiredEventQueue.getFirst() == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("CurrentEventQueue", currentEventQueue.getSnapshot());
            state.put("ExpiredEventQueue", expiredEventQueue.getSnapshot());
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            currentEventQueue.clear();
            currentEventQueue.restore((SnapshotStateList) state.get("CurrentEventQueue"));
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

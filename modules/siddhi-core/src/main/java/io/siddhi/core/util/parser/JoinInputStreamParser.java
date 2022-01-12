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
package io.siddhi.core.util.parser;

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.exception.QueryableRecordTableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.MultiProcessStreamReceiver;
import io.siddhi.core.query.input.ProcessStreamReceiver;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.join.JoinProcessor;
import io.siddhi.core.query.input.stream.join.JoinStreamRuntime;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.window.AggregateWindowProcessor;
import io.siddhi.core.query.processor.stream.window.EmptyWindowProcessor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.processor.stream.window.TableWindowProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.query.processor.stream.window.WindowWindowProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.siddhi.core.event.stream.MetaStreamEvent.EventType.AGGREGATE;
import static io.siddhi.core.event.stream.MetaStreamEvent.EventType.TABLE;
import static io.siddhi.core.event.stream.MetaStreamEvent.EventType.WINDOW;

public class JoinInputStreamParser {
    private static final Logger log = LogManager.getLogger(JoinInputStreamParser.class);


    public static StreamRuntime parseInputStream(JoinInputStream joinInputStream, Query query,
                                                 Map<String, AbstractDefinition> streamDefinitionMap,
                                                 Map<String, AbstractDefinition> tableDefinitionMap,
                                                 Map<String, AbstractDefinition> windowDefinitionMap,
                                                 Map<String, AbstractDefinition> aggregationDefinitionMap,
                                                 Map<String, Table> tableMap,
                                                 Map<String, Window> windowMap,
                                                 Map<String, AggregationRuntime> aggregationMap,
                                                 List<VariableExpressionExecutor> executors,
                                                 boolean outputExpectsExpiredEvents,
                                                 SiddhiQueryContext siddhiQueryContext) {
        try {
            ProcessStreamReceiver leftProcessStreamReceiver;
            ProcessStreamReceiver rightProcessStreamReceiver;

            MetaStreamEvent leftMetaStreamEvent = new MetaStreamEvent();
            MetaStreamEvent rightMetaStreamEvent = new MetaStreamEvent();

            String leftInputStreamId = ((SingleInputStream) joinInputStream.getLeftInputStream()).getStreamId();
            String rightInputStreamId = ((SingleInputStream) joinInputStream.getRightInputStream()).getStreamId();

            boolean leftOuterJoinProcessor = false;
            boolean rightOuterJoinProcessor = false;

            if (joinInputStream.getAllStreamIds().size() == 2) {

                setEventType(streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, aggregationDefinitionMap,
                        leftMetaStreamEvent, leftInputStreamId);
                setEventType(streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, aggregationDefinitionMap,
                        rightMetaStreamEvent, rightInputStreamId);
                leftProcessStreamReceiver = new ProcessStreamReceiver(leftInputStreamId,
                        siddhiQueryContext);
                rightProcessStreamReceiver = new ProcessStreamReceiver(rightInputStreamId,
                        siddhiQueryContext);
                if ((leftMetaStreamEvent.getEventType() == TABLE || leftMetaStreamEvent.getEventType() == AGGREGATE) &&
                        (rightMetaStreamEvent.getEventType() == TABLE ||
                                rightMetaStreamEvent.getEventType() == AGGREGATE)) {
                    throw new SiddhiAppCreationException("Both inputs of join " +
                            leftInputStreamId + " and " + rightInputStreamId + " are from static sources");
                }
                if (leftMetaStreamEvent.getEventType() != AGGREGATE &&
                        rightMetaStreamEvent.getEventType() != AGGREGATE) {
                    if (joinInputStream.getPer() != null) {
                        throw new SiddhiAppCreationException("When joining " + leftInputStreamId + " and " +
                                rightInputStreamId + " 'per' cannot be used as neither of them is an aggregation ");
                    } else if (joinInputStream.getWithin() != null) {
                        throw new SiddhiAppCreationException("When joining " + leftInputStreamId + " and " +
                                rightInputStreamId + " 'within' cannot be used as neither of them is an aggregation ");
                    }
                }
            } else {
                if (windowDefinitionMap.containsKey(joinInputStream.getAllStreamIds().get(0))) {
                    leftMetaStreamEvent.setEventType(WINDOW);
                    rightMetaStreamEvent.setEventType(WINDOW);
                    rightProcessStreamReceiver = new MultiProcessStreamReceiver(
                            joinInputStream.getAllStreamIds().get(0), 1, new Object(), siddhiQueryContext);
                    leftProcessStreamReceiver = rightProcessStreamReceiver;
                } else if (streamDefinitionMap.containsKey(joinInputStream.getAllStreamIds().get(0))) {
                    rightProcessStreamReceiver = new MultiProcessStreamReceiver(
                            joinInputStream.getAllStreamIds().get(0), 2, new Object(), siddhiQueryContext);
                    leftProcessStreamReceiver = rightProcessStreamReceiver;
                } else {
                    throw new SiddhiAppCreationException("Input of join is from static source " + leftInputStreamId +
                            " and " + rightInputStreamId);
                }
            }

            SingleStreamRuntime leftStreamRuntime = SingleInputStreamParser.parseInputStream(
                    (SingleInputStream) joinInputStream.getLeftInputStream(), executors,
                    streamDefinitionMap,
                    leftMetaStreamEvent.getEventType() != TABLE ? null : tableDefinitionMap,
                    leftMetaStreamEvent.getEventType() != WINDOW ? null : windowDefinitionMap,
                    leftMetaStreamEvent.getEventType() != AGGREGATE ? null : aggregationDefinitionMap,
                    tableMap, leftMetaStreamEvent, leftProcessStreamReceiver, true,
                    outputExpectsExpiredEvents, true, false, siddhiQueryContext);

            for (VariableExpressionExecutor variableExpressionExecutor : executors) {
                variableExpressionExecutor.getPosition()[SiddhiConstants.STREAM_EVENT_CHAIN_INDEX] = 0;
            }
            int size = executors.size();

            SingleStreamRuntime rightStreamRuntime = SingleInputStreamParser.parseInputStream(
                    (SingleInputStream) joinInputStream.getRightInputStream(), executors,
                    streamDefinitionMap,
                    rightMetaStreamEvent.getEventType() != TABLE ? null : tableDefinitionMap,
                    rightMetaStreamEvent.getEventType() != WINDOW ? null : windowDefinitionMap,
                    rightMetaStreamEvent.getEventType() != AGGREGATE ? null : aggregationDefinitionMap,
                    tableMap, rightMetaStreamEvent, rightProcessStreamReceiver, true,
                    outputExpectsExpiredEvents, true, false, siddhiQueryContext);

            for (int i = size; i < executors.size(); i++) {
                VariableExpressionExecutor variableExpressionExecutor = executors.get(i);
                variableExpressionExecutor.getPosition()[SiddhiConstants.STREAM_EVENT_CHAIN_INDEX] = 1;
            }

            setStreamRuntimeProcessorChain(leftMetaStreamEvent, leftStreamRuntime, leftInputStreamId, tableMap,
                    windowMap, aggregationMap, executors, outputExpectsExpiredEvents,
                    joinInputStream.getWithin(), joinInputStream.getPer(), query.getSelector().getGroupByList(),
                    siddhiQueryContext, joinInputStream.getLeftInputStream());
            setStreamRuntimeProcessorChain(rightMetaStreamEvent, rightStreamRuntime, rightInputStreamId, tableMap,
                    windowMap, aggregationMap, executors, outputExpectsExpiredEvents,
                    joinInputStream.getWithin(), joinInputStream.getPer(), query.getSelector().getGroupByList(),
                    siddhiQueryContext, joinInputStream.getRightInputStream());

            MetaStateEvent metaStateEvent = new MetaStateEvent(2);
            metaStateEvent.addEvent(leftMetaStreamEvent);
            metaStateEvent.addEvent(rightMetaStreamEvent);

            switch (joinInputStream.getType()) {
                case FULL_OUTER_JOIN:
                    leftOuterJoinProcessor = true;
                    rightOuterJoinProcessor = true;
                    break;
                case RIGHT_OUTER_JOIN:
                    rightOuterJoinProcessor = true;
                    break;
                case LEFT_OUTER_JOIN:
                    leftOuterJoinProcessor = true;
                    break;
            }

            JoinProcessor leftPreJoinProcessor = new JoinProcessor(true, true, leftOuterJoinProcessor, 0,
                    siddhiQueryContext.getSiddhiAppContext().getName(), siddhiQueryContext.getName());
            JoinProcessor leftPostJoinProcessor = new JoinProcessor(true, false, leftOuterJoinProcessor, 0,
                    siddhiQueryContext.getSiddhiAppContext().getName(), siddhiQueryContext.getName());

            FindableProcessor leftFindableProcessor = insertJoinProcessorsAndGetFindable(leftPreJoinProcessor,
                    leftPostJoinProcessor, leftStreamRuntime, outputExpectsExpiredEvents,
                    joinInputStream.getLeftInputStream(), siddhiQueryContext
            );

            JoinProcessor rightPreJoinProcessor = new JoinProcessor(false, true, rightOuterJoinProcessor, 1,
                    siddhiQueryContext.getSiddhiAppContext().getName(), siddhiQueryContext.getName());
            JoinProcessor rightPostJoinProcessor = new JoinProcessor(false, false, rightOuterJoinProcessor, 1,
                    siddhiQueryContext.getSiddhiAppContext().getName(), siddhiQueryContext.getName());

            FindableProcessor rightFindableProcessor = insertJoinProcessorsAndGetFindable(rightPreJoinProcessor,
                    rightPostJoinProcessor, rightStreamRuntime, outputExpectsExpiredEvents,
                    joinInputStream.getRightInputStream(), siddhiQueryContext
            );

            leftPreJoinProcessor.setFindableProcessor(rightFindableProcessor);
            leftPostJoinProcessor.setFindableProcessor(rightFindableProcessor);

            rightPreJoinProcessor.setFindableProcessor(leftFindableProcessor);
            rightPostJoinProcessor.setFindableProcessor(leftFindableProcessor);

            Expression compareCondition = joinInputStream.getOnCompare();
            if (compareCondition == null) {
                compareCondition = Expression.value(true);
            }
            QuerySelector querySelector = null;
            if (!(rightFindableProcessor instanceof TableWindowProcessor ||
                    rightFindableProcessor instanceof AggregateWindowProcessor) &&
                    (joinInputStream.getTrigger() != JoinInputStream.EventTrigger.LEFT)) {
                MatchingMetaInfoHolder leftMatchingMetaInfoHolder = MatcherParser.constructMatchingMetaStateHolder
                        (metaStateEvent, 1, leftMetaStreamEvent.getLastInputDefinition(),
                                SiddhiConstants.UNKNOWN_STATE);
                CompiledCondition rightCompiledCondition = leftFindableProcessor.compileCondition(compareCondition,
                        leftMatchingMetaInfoHolder, executors, tableMap, siddhiQueryContext);
                List<Attribute> expectedOutputAttributes = new ArrayList<>();
                CompiledSelection rightCompiledSelection = null;
                if (leftFindableProcessor instanceof TableWindowProcessor &&
                        ((TableWindowProcessor) leftFindableProcessor).isOptimisableLookup()) {

                    querySelector = SelectorParser.parse(query.getSelector(), query.getOutputStream(), metaStateEvent,
                            tableMap, executors, SiddhiConstants.UNKNOWN_STATE, ProcessingMode.BATCH,
                            outputExpectsExpiredEvents, siddhiQueryContext);

                    expectedOutputAttributes = metaStateEvent.getOutputStreamDefinition().getAttributeList();

                    try {
                        rightCompiledSelection = ((QueryableProcessor) leftFindableProcessor).compileSelection(
                                query.getSelector(), expectedOutputAttributes, leftMatchingMetaInfoHolder, executors,
                                tableMap, siddhiQueryContext
                        );
                    } catch (SiddhiAppCreationException | SiddhiAppValidationException | QueryableRecordTableException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Performing select clause in databases failed for query: '" +
                                    siddhiQueryContext.getName() + "' within Siddhi app '" +
                                    siddhiQueryContext.getSiddhiAppContext().getName() + "' hence reverting back to " +
                                    "querying only with where clause. Reason for failure: " + e.getMessage(), e);
                        }
                        // Nothing to override
                    }
                }
                populateJoinProcessors(rightMetaStreamEvent, rightInputStreamId, rightPreJoinProcessor,
                        rightPostJoinProcessor, rightCompiledCondition, rightCompiledSelection,
                        expectedOutputAttributes);
            }
            if (!(leftFindableProcessor instanceof TableWindowProcessor ||
                    leftFindableProcessor instanceof AggregateWindowProcessor) &&
                    (joinInputStream.getTrigger() != JoinInputStream.EventTrigger.RIGHT)) {
                MatchingMetaInfoHolder rightMatchingMetaInfoHolder = MatcherParser.constructMatchingMetaStateHolder
                        (metaStateEvent, 0, rightMetaStreamEvent.getLastInputDefinition(),
                                SiddhiConstants.UNKNOWN_STATE);
                CompiledCondition leftCompiledCondition = rightFindableProcessor.compileCondition(compareCondition,
                        rightMatchingMetaInfoHolder, executors, tableMap, siddhiQueryContext);
                List<Attribute> expectedOutputAttributes = new ArrayList<>();
                CompiledSelection leftCompiledSelection = null;
                if (rightFindableProcessor instanceof TableWindowProcessor &&
                        ((TableWindowProcessor) rightFindableProcessor).isOptimisableLookup()) {

                    querySelector = SelectorParser.parse(query.getSelector(), query.getOutputStream(), metaStateEvent,
                            tableMap, executors, SiddhiConstants.UNKNOWN_STATE, ProcessingMode.BATCH,
                            outputExpectsExpiredEvents, siddhiQueryContext);

                    expectedOutputAttributes = metaStateEvent.getOutputStreamDefinition().getAttributeList();

                    try {
                        leftCompiledSelection = ((QueryableProcessor) rightFindableProcessor).compileSelection(
                                query.getSelector(), expectedOutputAttributes, rightMatchingMetaInfoHolder, executors,
                                tableMap, siddhiQueryContext
                        );
                    } catch (SiddhiAppCreationException | SiddhiAppValidationException | QueryableRecordTableException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Performing select clause in databases failed for query: '" +
                                    siddhiQueryContext.getName() + "' within Siddhi app '" +
                                    siddhiQueryContext.getSiddhiAppContext().getName() + "' hence reverting back to " +
                                    "querying only with where clause. Reason for failure: " + e.getMessage(), e);
                        }
                        // Nothing to override
                    }
                }
                populateJoinProcessors(leftMetaStreamEvent, leftInputStreamId, leftPreJoinProcessor,
                        leftPostJoinProcessor, leftCompiledCondition, leftCompiledSelection, expectedOutputAttributes);
            }
            JoinStreamRuntime joinStreamRuntime = new JoinStreamRuntime(siddhiQueryContext, metaStateEvent);
            joinStreamRuntime.addRuntime(leftStreamRuntime);
            joinStreamRuntime.addRuntime(rightStreamRuntime);
            joinStreamRuntime.setQuerySelector(querySelector);
            return joinStreamRuntime;
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, joinInputStream, siddhiQueryContext.getSiddhiAppContext(),
                    siddhiQueryContext);
            throw t;
        }
    }

    private static void setEventType(Map<String, AbstractDefinition> streamDefinitionMap,
                                     Map<String, AbstractDefinition> tableDefinitionMap,
                                     Map<String, AbstractDefinition> windowDefinitionMap,
                                     Map<String, AbstractDefinition> aggregationDefinitionMap,
                                     MetaStreamEvent metaStreamEvent, String inputStreamId) {
        if (windowDefinitionMap.containsKey(inputStreamId)) {
            metaStreamEvent.setEventType(WINDOW);
        } else if (tableDefinitionMap.containsKey(inputStreamId)) {
            metaStreamEvent.setEventType(TABLE);
        } else if (aggregationDefinitionMap.containsKey(inputStreamId)) {
            metaStreamEvent.setEventType(AGGREGATE);
        } else if (!streamDefinitionMap.containsKey(inputStreamId)) {
            throw new SiddhiParserException("Definition of \"" + inputStreamId + "\" is not given");
        }
    }

    private static void populateJoinProcessors(MetaStreamEvent metaStreamEvent, String inputStreamId,
                                               JoinProcessor preJoinProcessor, JoinProcessor postJoinProcessor,
                                               CompiledCondition compiledCondition,
                                               CompiledSelection compiledSelection,
                                               List<Attribute> expectedOutputAttributes) {
        if (metaStreamEvent.getEventType() == TABLE && metaStreamEvent.getEventType() == AGGREGATE) {
            throw new SiddhiAppCreationException(inputStreamId + " of join query cannot trigger join " +
                    "because its a " + metaStreamEvent.getEventType() + ", only WINDOW and STEAM can " +
                    "trigger join");
        }
        preJoinProcessor.setTrigger(false);    // Pre JoinProcessor does not process the events
        preJoinProcessor.setCompiledCondition(compiledCondition);
        preJoinProcessor.setCompiledSelection(compiledSelection);
        preJoinProcessor.setExpectedOutputAttributes(expectedOutputAttributes);
        postJoinProcessor.setTrigger(true);
        postJoinProcessor.setCompiledCondition(compiledCondition);
        postJoinProcessor.setCompiledSelection(compiledSelection);
        postJoinProcessor.setExpectedOutputAttributes(expectedOutputAttributes);
    }

    private static void setStreamRuntimeProcessorChain(
            MetaStreamEvent metaStreamEvent, SingleStreamRuntime streamRuntime,
            String inputStreamId, Map<String, Table> tableMap, Map<String, Window> windowMap,
            Map<String, AggregationRuntime> aggregationMap,
            List<VariableExpressionExecutor> variableExpressionExecutors, boolean outputExpectsExpiredEvents,
            Within within, Expression per, List<Variable> queryGroupByList, SiddhiQueryContext siddhiQueryContext,
            InputStream inputStream) {
        switch (metaStreamEvent.getEventType()) {

            case TABLE:
                TableWindowProcessor tableWindowProcessor = new TableWindowProcessor(tableMap.get(inputStreamId));
                tableWindowProcessor.initProcessor(metaStreamEvent,
                        new ExpressionExecutor[0], null, outputExpectsExpiredEvents,
                        true, false, inputStream, siddhiQueryContext);
                streamRuntime.setProcessorChain(tableWindowProcessor);
                break;
            case WINDOW:
                WindowWindowProcessor windowWindowProcessor = new WindowWindowProcessor(
                        windowMap.get(inputStreamId));
                windowWindowProcessor.initProcessor(metaStreamEvent,
                        variableExpressionExecutors.toArray(new ExpressionExecutor[0]), null,
                        outputExpectsExpiredEvents, true, false, inputStream, siddhiQueryContext);
                streamRuntime.setProcessorChain(windowWindowProcessor);
                break;
            case AGGREGATE:

                AggregationRuntime aggregationRuntime = aggregationMap.get(inputStreamId);
                AggregateWindowProcessor aggregateWindowProcessor = new AggregateWindowProcessor(
                        aggregationRuntime, within, per, queryGroupByList);
                aggregateWindowProcessor.initProcessor(metaStreamEvent,
                        variableExpressionExecutors.toArray(new ExpressionExecutor[0]), null,
                        outputExpectsExpiredEvents, true, false, inputStream, siddhiQueryContext);
                streamRuntime.setProcessorChain(aggregateWindowProcessor);
                break;
            case DEFAULT:
                break;
        }
    }

    private static FindableProcessor insertJoinProcessorsAndGetFindable(JoinProcessor preJoinProcessor,
                                                                        JoinProcessor postJoinProcessor,
                                                                        SingleStreamRuntime streamRuntime,
                                                                        boolean outputExpectsExpiredEvents,
                                                                        InputStream inputStream,
                                                                        SiddhiQueryContext siddhiQueryContext) {

        Processor lastProcessor = streamRuntime.getProcessorChain();
        Processor prevLastProcessor = null;
        boolean containFindable = false;
        if (lastProcessor != null) {
            containFindable = lastProcessor instanceof FindableProcessor;
            while (lastProcessor.getNextProcessor() != null) {
                prevLastProcessor = lastProcessor;
                lastProcessor = lastProcessor.getNextProcessor();
                if (!containFindable) {
                    containFindable = lastProcessor instanceof FindableProcessor;
                }
            }
        }

        if (!containFindable) {
            try {
                WindowProcessor windowProcessor = new EmptyWindowProcessor();
                ExpressionExecutor[] expressionExecutors = new ExpressionExecutor[1];
                expressionExecutors[0] = new ConstantExpressionExecutor(0, Attribute.Type.INT);
                ConfigReader configReader = siddhiQueryContext.getSiddhiContext()
                        .getConfigManager().generateConfigReader("", "lengthBatch");
                windowProcessor.initProcessor(
                        ((MetaStreamEvent) streamRuntime.getMetaComplexEvent()),
                        expressionExecutors, configReader, outputExpectsExpiredEvents,
                        true, false, inputStream, siddhiQueryContext);
                if (lastProcessor != null) {
                    prevLastProcessor = lastProcessor;
                    prevLastProcessor.setNextProcessor(windowProcessor);
                    lastProcessor = windowProcessor;
                } else {
                    lastProcessor = windowProcessor;
                }
            } catch (Throwable t) {
                throw new SiddhiAppCreationException(t);
            }
        }
        if (lastProcessor instanceof FindableProcessor) {
            if (prevLastProcessor != null) {
                prevLastProcessor.setNextProcessor(preJoinProcessor);
            } else {
                streamRuntime.setProcessorChain(preJoinProcessor);
            }
            preJoinProcessor.setNextProcessor(lastProcessor);
            lastProcessor.setNextProcessor(postJoinProcessor);
            return (FindableProcessor) lastProcessor;
        } else {
            throw new OperationNotSupportedException("Stream " + ((MetaStreamEvent) streamRuntime.getMetaComplexEvent
                    ()).getLastInputDefinition().getId() +
                    "'s last processor " + lastProcessor.getClass().getCanonicalName() + " is not an instance of " +
                    FindableProcessor.class.getCanonicalName() + " hence join cannot be proceed");
        }

    }
}

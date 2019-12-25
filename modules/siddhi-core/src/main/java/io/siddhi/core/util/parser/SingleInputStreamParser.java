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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.ProcessStreamReceiver;
import io.siddhi.core.query.input.stream.single.EntryValveProcessor;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.filter.FilterProcessor;
import io.siddhi.core.query.processor.stream.AbstractStreamProcessor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiClassLoader;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.holder.StreamFunctionProcessorExtensionHolder;
import io.siddhi.core.util.extension.holder.StreamProcessorExtensionHolder;
import io.siddhi.core.util.extension.holder.WindowProcessorExtensionHolder;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.execution.query.input.handler.Filter;
import io.siddhi.query.api.execution.query.input.handler.StreamFunction;
import io.siddhi.query.api.execution.query.input.handler.StreamHandler;
import io.siddhi.query.api.execution.query.input.handler.Window;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.extension.Extension;

import java.util.List;
import java.util.Map;

public class SingleInputStreamParser {

    /**
     * Parse single InputStream and return SingleStreamRuntime
     *
     * @param inputStream                 single input stream to be parsed
     * @param variableExpressionExecutors List to hold VariableExpressionExecutors to update after query parsing
     * @param streamDefinitionMap         Stream Definition Map
     * @param tableDefinitionMap          Table Definition Map
     * @param windowDefinitionMap         window definition map
     * @param aggregationDefinitionMap    aggregation definition map
     * @param tableMap                    Table Map
     * @param metaComplexEvent            MetaComplexEvent
     * @param processStreamReceiver       ProcessStreamReceiver
     * @param supportsBatchProcessing     supports batch processing
     * @param outputExpectsExpiredEvents  is expired events sent as output
     * @param findToBeExecuted            find will be executed in the steam stores
     * @param multiValue                  event has the possibility to produce multiple values
     * @param siddhiQueryContext          @return SingleStreamRuntime
     */
    public static SingleStreamRuntime parseInputStream(SingleInputStream inputStream,
                                                       List<VariableExpressionExecutor> variableExpressionExecutors,
                                                       Map<String, AbstractDefinition> streamDefinitionMap,
                                                       Map<String, AbstractDefinition> tableDefinitionMap,
                                                       Map<String, AbstractDefinition> windowDefinitionMap,
                                                       Map<String, AbstractDefinition> aggregationDefinitionMap,
                                                       Map<String, Table> tableMap,
                                                       MetaComplexEvent metaComplexEvent,
                                                       ProcessStreamReceiver processStreamReceiver,
                                                       boolean supportsBatchProcessing,
                                                       boolean outputExpectsExpiredEvents,
                                                       boolean findToBeExecuted,
                                                       boolean multiValue,
                                                       SiddhiQueryContext siddhiQueryContext) {
        Processor processor = null;
        EntryValveProcessor entryValveProcessor = null;
        ProcessingMode processingMode = ProcessingMode.BATCH;
        boolean first = true;
        MetaStreamEvent metaStreamEvent;
        if (metaComplexEvent instanceof MetaStateEvent) {
            metaStreamEvent = new MetaStreamEvent();
            ((MetaStateEvent) metaComplexEvent).addEvent(metaStreamEvent);
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap,
                    aggregationDefinitionMap, multiValue, metaStreamEvent);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaComplexEvent;
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap,
                    aggregationDefinitionMap, multiValue, metaStreamEvent);
        }

        // A window cannot be defined for a window stream
        if (!inputStream.getStreamHandlers().isEmpty() && windowDefinitionMap != null && windowDefinitionMap
                .containsKey(inputStream.getStreamId())) {
            for (StreamHandler handler : inputStream.getStreamHandlers()) {
                if (handler instanceof Window) {
                    throw new OperationNotSupportedException("Cannot create " + ((Window) handler).getName() + " " +
                            "window for the window stream " + inputStream.getStreamId());
                }
            }
        }

        if (!inputStream.getStreamHandlers().isEmpty()) {
            for (StreamHandler handler : inputStream.getStreamHandlers()) {
                Processor currentProcessor = generateProcessor(handler, metaComplexEvent,
                        variableExpressionExecutors, tableMap, supportsBatchProcessing,
                        outputExpectsExpiredEvents, findToBeExecuted, siddhiQueryContext);
                if (currentProcessor instanceof SchedulingProcessor) {
                    if (entryValveProcessor == null) {

                        entryValveProcessor = new EntryValveProcessor(siddhiQueryContext.getSiddhiAppContext());
                        if (first) {
                            processor = entryValveProcessor;
                            first = false;
                        } else {
                            processor.setToLast(entryValveProcessor);
                        }
                    }
                    Scheduler scheduler = SchedulerParser.parse(entryValveProcessor, siddhiQueryContext);
                    ((SchedulingProcessor) currentProcessor).setScheduler(scheduler);
                }
                if (currentProcessor instanceof AbstractStreamProcessor) {
                    processingMode = ProcessingMode.findUpdatedProcessingMode(processingMode,
                            ((AbstractStreamProcessor) currentProcessor).getProcessingMode());
                }
                if (first) {
                    processor = currentProcessor;
                    first = false;
                } else {
                    processor.setToLast(currentProcessor);
                }
            }
        }

        metaStreamEvent.initializeOnAfterWindowData();
        return new SingleStreamRuntime(processStreamReceiver, processor, processingMode, metaComplexEvent);

    }


    public static Processor generateProcessor(StreamHandler streamHandler, MetaComplexEvent metaEvent,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap,
                                              boolean supportsBatchProcessing, boolean outputExpectsExpiredEvents,
                                              boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        Expression[] parameters = streamHandler.getParameters();
        MetaStreamEvent metaStreamEvent;
        int stateIndex = SiddhiConstants.UNKNOWN_STATE;
        if (metaEvent instanceof MetaStateEvent) {
            stateIndex = ((MetaStateEvent) metaEvent).getStreamEventCount() - 1;
            metaStreamEvent = ((MetaStateEvent) metaEvent).getMetaStreamEvent(stateIndex);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaEvent;
        }

        if (streamHandler instanceof Window) {
            metaStreamEvent.initializeOnAfterWindowData();
        }

        ExpressionExecutor[] attributeExpressionExecutors;
        if (parameters != null) {
            if (parameters.length > 0) {
                attributeExpressionExecutors = new ExpressionExecutor[parameters.length];
                for (int i = 0, parametersLength = parameters.length; i < parametersLength; i++) {
                    attributeExpressionExecutors[i] = ExpressionParser.parseExpression(parameters[i], metaEvent,
                            stateIndex, tableMap, variableExpressionExecutors, false,
                            SiddhiConstants.CURRENT, ProcessingMode.BATCH, false,
                            siddhiQueryContext);
                }
            } else {
                List<Attribute> attributeList = metaStreamEvent.getLastInputDefinition().getAttributeList();
                int parameterSize = attributeList.size();
                attributeExpressionExecutors = new ExpressionExecutor[parameterSize];
                for (int i = 0; i < parameterSize; i++) {
                    attributeExpressionExecutors[i] = ExpressionParser.parseExpression(new Variable(attributeList.get
                                    (i).getName()), metaEvent, stateIndex, tableMap, variableExpressionExecutors,
                            false, SiddhiConstants.CURRENT, ProcessingMode.BATCH,
                            false, siddhiQueryContext);
                }
            }
        } else {
            attributeExpressionExecutors = new ExpressionExecutor[0];
        }

        ConfigReader configReader;
        if (streamHandler instanceof Filter) {
            return new FilterProcessor(attributeExpressionExecutors[0]);

        } else if (streamHandler instanceof Window) {
            WindowProcessor windowProcessor = (WindowProcessor) SiddhiClassLoader.loadExtensionImplementation(
                    (Extension) streamHandler,
                    WindowProcessorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
            configReader = siddhiQueryContext.getSiddhiContext().getConfigManager().
                    generateConfigReader(((Window) streamHandler).getNamespace(),
                            ((Window) streamHandler).getName());
            windowProcessor.initProcessor(metaStreamEvent, attributeExpressionExecutors,
                    configReader, outputExpectsExpiredEvents, findToBeExecuted, false, streamHandler, siddhiQueryContext);
            return windowProcessor;

        } else if (streamHandler instanceof StreamFunction) {
            AbstractStreamProcessor abstractStreamProcessor;
            configReader = siddhiQueryContext.getSiddhiContext().getConfigManager().
                    generateConfigReader(((StreamFunction) streamHandler).getNamespace(),
                            ((StreamFunction) streamHandler).getName());
            if (supportsBatchProcessing) {
                try {
                    abstractStreamProcessor = (StreamProcessor) SiddhiClassLoader.loadExtensionImplementation(
                            (Extension) streamHandler,
                            StreamProcessorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
                    abstractStreamProcessor.initProcessor(metaStreamEvent,
                            attributeExpressionExecutors, configReader,
                            outputExpectsExpiredEvents, false, false, streamHandler, siddhiQueryContext);
                    return abstractStreamProcessor;
                } catch (SiddhiAppCreationException e) {
                    if (!e.isClassLoadingIssue()) {
                        ExceptionUtil.populateQueryContext(e, streamHandler, siddhiQueryContext.getSiddhiAppContext(),
                                siddhiQueryContext);
                        throw e;
                    }
                }
            }
            abstractStreamProcessor = (StreamFunctionProcessor) SiddhiClassLoader.loadExtensionImplementation(
                    (Extension) streamHandler,
                    StreamFunctionProcessorExtensionHolder.getInstance(siddhiQueryContext.getSiddhiAppContext()));
            abstractStreamProcessor.initProcessor(metaStreamEvent, attributeExpressionExecutors,
                    configReader, outputExpectsExpiredEvents, false, false, streamHandler, siddhiQueryContext);
            return abstractStreamProcessor;
        } else {
            throw new SiddhiAppCreationException(streamHandler.getClass().getName() + " is not supported",
                    streamHandler.getQueryContextStartIndex(), streamHandler.getQueryContextEndIndex());
        }
    }

    /**
     * Method to generate MetaStreamEvent reagent to the given input stream. Empty definition will be created and
     * definition and reference is will be set accordingly in this method.
     *
     * @param inputStream              InputStream
     * @param streamDefinitionMap      StreamDefinition Map
     * @param tableDefinitionMap       TableDefinition Map
     * @param aggregationDefinitionMap AggregationDefinition Map
     * @param multiValue               Event has the possibility to produce multiple values
     * @param metaStreamEvent          MetaStreamEvent
     */
    private static void initMetaStreamEvent(SingleInputStream inputStream,
                                            Map<String, AbstractDefinition> streamDefinitionMap,
                                            Map<String, AbstractDefinition> tableDefinitionMap,
                                            Map<String, AbstractDefinition> windowDefinitionMap,
                                            Map<String, AbstractDefinition> aggregationDefinitionMap,
                                            boolean multiValue, MetaStreamEvent metaStreamEvent) {
        String streamId = inputStream.getStreamId();

        if (!inputStream.isInnerStream() && windowDefinitionMap != null && windowDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = windowDefinitionMap.get(streamId);
            if (!metaStreamEvent.getInputDefinitions().contains(inputDefinition)) {
                metaStreamEvent.addInputDefinition(inputDefinition);
            }
        } else if (streamDefinitionMap != null && streamDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = streamDefinitionMap.get(streamId);
            metaStreamEvent.addInputDefinition(inputDefinition);
        } else if (!inputStream.isInnerStream() && tableDefinitionMap != null &&
                tableDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = tableDefinitionMap.get(streamId);
            metaStreamEvent.addInputDefinition(inputDefinition);
        } else if (!inputStream.isInnerStream() && aggregationDefinitionMap != null &&
                aggregationDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = aggregationDefinitionMap.get(streamId);
            metaStreamEvent.addInputDefinition(inputDefinition);
        } else {
            throw new SiddhiAppCreationException("Stream/table/window/aggregation definition with ID '" +
                    inputStream.getStreamId() + "' has not been defined", inputStream.getQueryContextStartIndex(),
                    inputStream.getQueryContextEndIndex());
        }

        if ((inputStream.getStreamReferenceId() != null) &&
                !(inputStream.getStreamId()).equals(inputStream.getStreamReferenceId())) { //if ref id is provided
            metaStreamEvent.setInputReferenceId(inputStream.getStreamReferenceId());
        }
        metaStreamEvent.setMultiValue(multiValue);
    }


}

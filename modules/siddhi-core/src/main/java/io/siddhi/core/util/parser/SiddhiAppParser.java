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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.partition.PartitionRuntimeImpl;
import io.siddhi.core.query.QueryRuntimeImpl;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.IdGenerator;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.ThreadBarrier;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.timestamp.TimestampGenerator;
import io.siddhi.core.util.timestamp.TimestampGeneratorImpl;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.FunctionDefinition;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.definition.TriggerDefinition;
import io.siddhi.query.api.definition.WindowDefinition;
import io.siddhi.query.api.exception.DuplicateAnnotationException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.ExecutionElement;
import io.siddhi.query.api.execution.partition.Partition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.handler.StreamHandler;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;
import io.siddhi.query.api.expression.condition.In;
import io.siddhi.query.api.util.AnnotationHelper;
import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Class to parse {@link SiddhiApp}
 */
public class SiddhiAppParser {
    private static final Logger log = LogManager.getLogger(SiddhiAppParser.class);

    /**
     * Parse an SiddhiApp returning SiddhiAppRuntime
     *
     * @param siddhiApp       plan to be parsed
     * @param siddhiAppString content of Siddhi application as string
     * @param siddhiContext   SiddhiContext  @return SiddhiAppRuntime
     * @return SiddhiAppRuntimeBuilder
     */
    public static SiddhiAppRuntimeBuilder parse(SiddhiApp siddhiApp, String siddhiAppString,
                                                SiddhiContext siddhiContext) {

        SiddhiAppContext siddhiAppContext = new SiddhiAppContext();
        siddhiAppContext.setSiddhiContext(siddhiContext);
        siddhiAppContext.setSiddhiAppString(siddhiAppString);
        siddhiAppContext.setSiddhiApp(siddhiApp);

        try {
            Element element = AnnotationHelper.getAnnotationElement(SiddhiConstants.ANNOTATION_NAME, null,
                    siddhiApp.getAnnotations());
            if (element != null) {
                siddhiAppContext.setName(element.getValue());
            } else {
                siddhiAppContext.setName(UUID.randomUUID().toString());
            }

            Annotation annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_ENFORCE_ORDER,
                    siddhiApp.getAnnotations());
            if (annotation != null) {
                siddhiAppContext.setEnforceOrder(true);
            }

            annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_ASYNC,
                    siddhiApp.getAnnotations());
            if (annotation != null) {
                throw new SiddhiAppCreationException("@Async not supported in SiddhiApp level, " +
                        "instead use @Async with streams",
                        annotation.getQueryContextStartIndex(), annotation.getQueryContextEndIndex());
            }

            annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STATISTICS,
                    siddhiApp.getAnnotations());

            List<Element> statisticsElements = new ArrayList<>();
            if (annotation != null) {
                statisticsElements = annotation.getElements();
            }
            if (siddhiContext.getStatisticsConfiguration() != null) {
                siddhiAppContext.setStatisticsManager(siddhiContext
                        .getStatisticsConfiguration()
                        .getFactory()
                        .createStatisticsManager(
                                siddhiContext.getStatisticsConfiguration().getMetricPrefix(),
                                siddhiAppContext.getName(),
                                statisticsElements));
            }

            Element statStateEnableElement = AnnotationHelper.getAnnotationElement(
                    SiddhiConstants.ANNOTATION_STATISTICS,
                    SiddhiConstants.ANNOTATION_ELEMENT_ENABLE, siddhiApp.getAnnotations());

            if (statStateEnableElement != null && Boolean.valueOf(statStateEnableElement.getValue())) {
                siddhiAppContext.setRootMetricsLevel(Level.BASIC);
            } else {
                Element statStateElement = AnnotationHelper.getAnnotationElement(
                        SiddhiConstants.ANNOTATION_STATISTICS, null, siddhiApp.getAnnotations());
                // Both annotation and statElement should be checked since siddhi uses
                // @app:statistics(reporter = 'console', interval = '5' )
                // where sp uses @app:statistics('true').
                if (annotation != null && (statStateElement == null || Boolean.valueOf(statStateElement.getValue()))) {
                    siddhiAppContext.setRootMetricsLevel(Level.BASIC);
                }
            }
            Element statStateIncludeElement = AnnotationHelper.getAnnotationElement(
                    SiddhiConstants.ANNOTATION_STATISTICS,
                    SiddhiConstants.ANNOTATION_ELEMENT_INCLUDE, siddhiApp.getAnnotations());
            siddhiAppContext.setIncludedMetrics(io.siddhi.core.util.parser.helper.AnnotationHelper.generateIncludedMetrics(statStateIncludeElement));

            Element transportCreationEnabledElement = AnnotationHelper.getAnnotationElement(
                    SiddhiConstants.TRANSPORT_CHANNEL_CREATION_IDENTIFIER, null, siddhiApp.getAnnotations());
            if (transportCreationEnabledElement == null) {
                siddhiAppContext.setTransportChannelCreationEnabled(true);
            } else {
                siddhiAppContext.setTransportChannelCreationEnabled(
                        Boolean.valueOf(transportCreationEnabledElement.getValue()));
            }


            siddhiAppContext.setThreadBarrier(new ThreadBarrier());

            siddhiAppContext.setExecutorService(Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("Siddhi-" + siddhiAppContext.getName() +
                            "-executor-thread-%d").build()));

            siddhiAppContext.setScheduledExecutorService(Executors.newScheduledThreadPool(5,
                    new ThreadFactoryBuilder().setNameFormat("Siddhi-" +
                            siddhiAppContext.getName() + "-scheduler-thread-%d").build()));

            // Select the TimestampGenerator based on playback mode on/off
            annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PLAYBACK,
                    siddhiApp.getAnnotations());
            if (annotation != null) {
                String idleTime = null;
                String increment = null;
                TimestampGenerator timestampGenerator = new TimestampGeneratorImpl(siddhiAppContext);
                // Get the optional elements of playback annotation
                for (Element e : annotation.getElements()) {
                    if (SiddhiConstants.ANNOTATION_ELEMENT_IDLE_TIME.equalsIgnoreCase(e.getKey())) {
                        idleTime = e.getValue();
                    } else if (SiddhiConstants.ANNOTATION_ELEMENT_INCREMENT.equalsIgnoreCase(e.getKey())) {
                        increment = e.getValue();
                    } else {
                        throw new SiddhiAppValidationException("Playback annotation accepts only idle.time and " +
                                "increment but found " + e.getKey());
                    }
                }

                // idleTime and increment are optional but if one presents, the other also should be given
                if (idleTime != null && increment == null) {
                    throw new SiddhiAppValidationException("Playback annotation requires both idle.time and " +
                            "increment but increment not found");
                } else if (idleTime == null && increment != null) {
                    throw new SiddhiAppValidationException("Playback annotation requires both idle.time and " +
                            "increment but idle.time does not found");
                } else if (idleTime != null) {
                    // The fourth case idleTime == null && increment == null are ignored because it means no heartbeat.
                    try {
                        timestampGenerator.setIdleTime(SiddhiCompiler.parseTimeConstantDefinition(idleTime).value());
                    } catch (SiddhiParserException ex) {
                        throw new SiddhiParserException("Invalid idle.time constant '" + idleTime + "' in playback " +
                                "annotation", ex);
                    }
                    try {
                        timestampGenerator.setIncrementInMilliseconds(SiddhiCompiler.parseTimeConstantDefinition
                                (increment).value());
                    } catch (SiddhiParserException ex) {
                        throw new SiddhiParserException("Invalid increment constant '" + increment + "' in playback " +
                                "annotation", ex);
                    }
                }

                siddhiAppContext.setTimestampGenerator(timestampGenerator);
                siddhiAppContext.setPlayback(true);
            } else {
                siddhiAppContext.setTimestampGenerator(new TimestampGeneratorImpl(siddhiAppContext));
            }
            siddhiAppContext.setSnapshotService(new SnapshotService(siddhiAppContext));
            siddhiAppContext.setIdGenerator(new IdGenerator());

        } catch (DuplicateAnnotationException e) {
            throw new DuplicateAnnotationException(e.getMessageWithOutContext() + " for the same Siddhi app " +
                    siddhiApp.toString(), e, e.getQueryContextStartIndex(), e.getQueryContextEndIndex(),
                    siddhiAppContext.getName(), siddhiAppContext.getSiddhiAppString());
        }

        SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder = new SiddhiAppRuntimeBuilder(siddhiAppContext);

        defineStreamDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getStreamDefinitionMap(), siddhiAppContext);
        defineTableDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getTableDefinitionMap(), siddhiAppContext);
        defineWindowDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getWindowDefinitionMap(), siddhiAppContext);
        defineFunctionDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getFunctionDefinitionMap(), siddhiAppContext);
        defineAggregationDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getAggregationDefinitionMap(),
                siddhiAppContext);
        //todo fix for query API usecase
        List<String> findExecutedElements = getFindExecutedElements(siddhiApp);
        for (Window window : siddhiAppRuntimeBuilder.getWindowMap().values()) {

            try {
                window.init(siddhiAppRuntimeBuilder.getTableMap(), siddhiAppRuntimeBuilder.getWindowMap(),
                        window.getWindowDefinition().getId(),
                        findExecutedElements.contains(window.getWindowDefinition().getId()));
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, window.getWindowDefinition(), siddhiAppContext);
                throw t;
            }
        }
        int queryIndex = 1;
        int partitionIndex = 1;
        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (executionElement instanceof Query) {
                try {
                    QueryRuntimeImpl queryRuntime = QueryParser.parse((Query) executionElement, siddhiAppContext,
                            siddhiAppRuntimeBuilder.getStreamDefinitionMap(),
                            siddhiAppRuntimeBuilder.getTableDefinitionMap(),
                            siddhiAppRuntimeBuilder.getWindowDefinitionMap(),
                            siddhiAppRuntimeBuilder.getAggregationDefinitionMap(),
                            siddhiAppRuntimeBuilder.getTableMap(),
                            siddhiAppRuntimeBuilder.getAggregationMap(),
                            siddhiAppRuntimeBuilder.getWindowMap(),
                            siddhiAppRuntimeBuilder.getLockSynchronizer(),
                            String.valueOf(queryIndex), false, SiddhiConstants.PARTITION_ID_DEFAULT);
                    siddhiAppRuntimeBuilder.addQuery(queryRuntime);
                    siddhiAppContext.addEternalReferencedHolder(queryRuntime);
                    queryIndex++;
                } catch (Throwable t) {
                    ExceptionUtil.populateQueryContext(t, (Query) executionElement, siddhiAppContext);
                    throw t;
                }
            } else {
                try {
                    PartitionRuntimeImpl partitionRuntime = PartitionParser.parse(siddhiAppRuntimeBuilder,
                            (Partition) executionElement, siddhiAppContext, queryIndex, partitionIndex);
                    siddhiAppRuntimeBuilder.addPartition(partitionRuntime);
                    queryIndex += ((Partition) executionElement).getQueryList().size();
                    partitionIndex++;
                } catch (Throwable t) {
                    ExceptionUtil.populateQueryContext(t, (Partition) executionElement, siddhiAppContext);
                    throw t;
                }
            }
        }
        //Done last as they have to be started last
        defineTriggerDefinitions(siddhiAppRuntimeBuilder, siddhiApp.getTriggerDefinitionMap(), siddhiAppContext);
        return siddhiAppRuntimeBuilder;
    }

    private static List<String> getFindExecutedElements(SiddhiApp siddhiApp) {
        List<String> findExecutedElements = new ArrayList<>();
        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {
            if (executionElement instanceof Query) {
                List<StreamHandler> streamHandlers = new ArrayList<>();
                if (((Query) executionElement).getInputStream() instanceof JoinInputStream) {
                    findExecutedElements.addAll(((Query) executionElement).getInputStream().getAllStreamIds());
                    streamHandlers.addAll(((SingleInputStream) ((JoinInputStream) ((Query) executionElement).getInputStream()).getLeftInputStream()).getStreamHandlers());
                    streamHandlers.addAll(((SingleInputStream) ((JoinInputStream) ((Query) executionElement).getInputStream()).getRightInputStream()).getStreamHandlers());
                } else if (((Query) executionElement).getInputStream() instanceof SingleInputStream) {
                    streamHandlers.addAll(((SingleInputStream) ((Query) executionElement).getInputStream()).getStreamHandlers());
                } else if (((Query) executionElement).getInputStream() instanceof StateInputStream) {
                    streamHandlers.addAll((((StateInputStream) ((Query) executionElement).getInputStream()).getStreamHandlers()));
                }
                for (StreamHandler streamHandler : streamHandlers) {
                    if (streamHandler instanceof In) {
                        findExecutedElements.add(((In) streamHandler).getSourceId());
                    }
                }
            } else {
                List<Query> queries = ((Partition) executionElement).getQueryList();
                for (Query query : queries) {
                    List<StreamHandler> streamHandlers = new ArrayList<>();
                    if (query.getInputStream() instanceof JoinInputStream) {
                        findExecutedElements.addAll(query.getInputStream().getAllStreamIds());
                        streamHandlers.addAll(((SingleInputStream) ((JoinInputStream) query.getInputStream()).getLeftInputStream()).getStreamHandlers());
                        streamHandlers.addAll(((SingleInputStream) ((JoinInputStream) query.getInputStream()).getRightInputStream()).getStreamHandlers());
                    } else if (query.getInputStream() instanceof SingleInputStream) {
                        streamHandlers.addAll(((SingleInputStream) query.getInputStream()).getStreamHandlers());
                    } else if (query.getInputStream() instanceof StateInputStream) {
                        streamHandlers.addAll((((StateInputStream) query.getInputStream()).getStreamHandlers()));
                    }
                    for (StreamHandler streamHandler : streamHandlers) {
                        if (streamHandler instanceof In) {
                            findExecutedElements.add(((In) streamHandler).getSourceId());
                        }
                    }
                }
            }

        }
        return findExecutedElements;
    }

    private static void defineTriggerDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                                 Map<String, TriggerDefinition> triggerDefinitionMap,
                                                 SiddhiAppContext siddhiAppContext) {
        for (TriggerDefinition definition : triggerDefinitionMap.values()) {
            try {
                siddhiAppRuntimeBuilder.defineTrigger(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }
    }

    private static void defineFunctionDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                                  Map<String, FunctionDefinition> functionDefinitionMap,
                                                  SiddhiAppContext siddhiAppContext) {
        for (FunctionDefinition definition : functionDefinitionMap.values()) {
            try {
                siddhiAppRuntimeBuilder.defineFunction(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }
    }

    private static void defineStreamDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                                Map<String, StreamDefinition> streamDefinitionMap,
                                                SiddhiAppContext siddhiAppContext) {
        for (StreamDefinition definition : streamDefinitionMap.values()) {
            try {
                Annotation onErrorAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_ON_ERROR,
                        definition.getAnnotations());
                if (onErrorAnnotation != null) {
                    StreamJunction.OnErrorAction onErrorAction = StreamJunction.OnErrorAction.valueOf(onErrorAnnotation
                            .getElement(SiddhiConstants.ANNOTATION_ELEMENT_ACTION).toUpperCase());
                    if (onErrorAction == StreamJunction.OnErrorAction.STREAM) {
                        StreamDefinition faultStreamDefinition = createFaultStreamDefinition(definition);
                        siddhiAppRuntimeBuilder.defineStream(faultStreamDefinition);
                    }
                }
                siddhiAppRuntimeBuilder.defineStream(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }
    }

    private static StreamDefinition createFaultStreamDefinition(StreamDefinition streamDefinition) {

        List<Attribute> attributeList = streamDefinition.getAttributeList();
        StreamDefinition faultStreamDefinition = new StreamDefinition();
        faultStreamDefinition.setId(SiddhiConstants.FAULT_STREAM_PREFIX.concat(streamDefinition.getId()));
        for (Attribute attribute : attributeList) {
            faultStreamDefinition.attribute(attribute.getName(), attribute.getType());
        }
        faultStreamDefinition.attribute("_error", Attribute.Type.OBJECT);

        faultStreamDefinition.setQueryContextStartIndex(streamDefinition.getQueryContextStartIndex());
        faultStreamDefinition.setQueryContextEndIndex(streamDefinition.getQueryContextEndIndex());
        return faultStreamDefinition;
    }

    private static void defineTableDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                               Map<String, TableDefinition> tableDefinitionMap,
                                               SiddhiAppContext siddhiAppContext) {
        for (TableDefinition definition : tableDefinitionMap.values()) {
            try {
                siddhiAppRuntimeBuilder.defineTable(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }
    }

    private static void defineWindowDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                                Map<String, WindowDefinition> windowDefinitionMap,
                                                SiddhiAppContext siddhiAppContext) {
        for (WindowDefinition definition : windowDefinitionMap.values()) {
            try {
                siddhiAppRuntimeBuilder.defineWindow(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }
    }

    private static void defineAggregationDefinitions(SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder,
                                                     Map<String, AggregationDefinition> aggregationDefinitionMap,
                                                     SiddhiAppContext siddhiAppContext) {
        for (AggregationDefinition definition : aggregationDefinitionMap.values()) {
            try {
                siddhiAppRuntimeBuilder.defineAggregation(definition);
            } catch (Throwable t) {
                ExceptionUtil.populateQueryContext(t, definition, siddhiAppContext);
                throw t;
            }
        }

    }
}

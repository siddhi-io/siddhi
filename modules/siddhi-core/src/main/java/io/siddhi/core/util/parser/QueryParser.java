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
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.MetaStreamEvent.EventType;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.QueryRuntime;
import io.siddhi.core.query.QueryRuntimeImpl;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.join.JoinProcessor;
import io.siddhi.core.query.input.stream.join.JoinStreamRuntime;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.snapshot.WrappedSnapshotOutputRateLimiter;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.Table;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.lock.LockSynchronizer;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.handler.StreamHandler;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.output.ratelimit.SnapshotOutputRate;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class to parse {@link QueryRuntime}.
 */
public class QueryParser {

    /**
     * Parse a query and return corresponding QueryRuntime.
     *
     * @param query                    query to be parsed.
     * @param siddhiAppContext         associated Siddhi app context.
     * @param streamDefinitionMap      keyvalue containing user given stream definitions.
     * @param tableDefinitionMap       keyvalue containing table definitions.
     * @param windowDefinitionMap      keyvalue containing window definition map.
     * @param aggregationDefinitionMap keyvalue containing aggregation definition map.
     * @param tableMap                 keyvalue containing event tables.
     * @param aggregationMap           keyvalue containing aggrigation runtimes.
     * @param windowMap                keyvalue containing event window map.
     * @param lockSynchronizer         Lock synchronizer for sync the lock across queries.
     * @param queryIndex               query index to identify unknown query by number
     * @param partitioned              is the query partitioned
     * @param partitionId              The ID of the partition
     * @return queryRuntime
     */
    public static QueryRuntimeImpl parse(Query query, SiddhiAppContext siddhiAppContext,
                                         Map<String, AbstractDefinition> streamDefinitionMap,
                                         Map<String, AbstractDefinition> tableDefinitionMap,
                                         Map<String, AbstractDefinition> windowDefinitionMap,
                                         Map<String, AbstractDefinition> aggregationDefinitionMap,
                                         Map<String, Table> tableMap,
                                         Map<String, AggregationRuntime> aggregationMap, Map<String, Window> windowMap,
                                         LockSynchronizer lockSynchronizer,
                                         String queryIndex, boolean partitioned, String partitionId) {
        List<VariableExpressionExecutor> executors = new ArrayList<>();
        QueryRuntimeImpl queryRuntime;
        Element nameElement = null;
        LatencyTracker latencyTracker = null;
        LockWrapper lockWrapper = null;
        try {
            nameElement = AnnotationHelper.getAnnotationElement("info", "name",
                    query.getAnnotations());
            String queryName;
            if (nameElement != null) {
                queryName = nameElement.getValue();
            } else {
                queryName = "query_" + queryIndex;
            }
            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, queryName, partitionId);
            siddhiQueryContext.setPartitioned(partitioned);
            latencyTracker = QueryParserHelper.createLatencyTracker(siddhiAppContext, siddhiQueryContext.getName(),
                    SiddhiConstants.METRIC_INFIX_QUERIES, null);
            siddhiQueryContext.setLatencyTracker(latencyTracker);

            OutputStream.OutputEventType outputEventType = query.getOutputStream().getOutputEventType();
            if (query.getOutputRate() != null && query.getOutputRate() instanceof SnapshotOutputRate) {
                if (outputEventType != OutputStream.OutputEventType.ALL_EVENTS) {
                    throw new SiddhiAppCreationException("As query '" + siddhiQueryContext.getName() +
                            "' is performing snapshot rate limiting, it can only insert '" +
                            OutputStream.OutputEventType.ALL_EVENTS +
                            "' but it is inserting '" + outputEventType + "'!",
                            query.getOutputStream().getQueryContextStartIndex(),
                            query.getOutputStream().getQueryContextEndIndex());
                }
            }
            siddhiQueryContext.setOutputEventType(outputEventType);

            boolean outputExpectsExpiredEvents = false;
            if (outputEventType != OutputStream.OutputEventType.CURRENT_EVENTS) {
                outputExpectsExpiredEvents = true;
            }
            StreamRuntime streamRuntime = InputStreamParser.parse(query.getInputStream(),
                    query, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap,
                    aggregationDefinitionMap, tableMap, windowMap, aggregationMap, executors,
                    outputExpectsExpiredEvents, siddhiQueryContext);
            QuerySelector selector;
            if (streamRuntime.getQuerySelector() != null) {
                selector = streamRuntime.getQuerySelector();
            } else {
                selector = SelectorParser.parse(query.getSelector(), query.getOutputStream(),
                        streamRuntime.getMetaComplexEvent(), tableMap, executors,
                        SiddhiConstants.UNKNOWN_STATE, streamRuntime.getProcessingMode(), outputExpectsExpiredEvents,
                        siddhiQueryContext);
            }
            boolean isWindow = query.getInputStream() instanceof JoinInputStream;
            if (!isWindow && query.getInputStream() instanceof SingleInputStream) {
                for (StreamHandler streamHandler : ((SingleInputStream) query.getInputStream()).getStreamHandlers()) {
                    if (streamHandler instanceof io.siddhi.query.api.execution.query.input.handler.Window) {
                        isWindow = true;
                        break;
                    }
                }
            }

            Element synchronizedElement = AnnotationHelper.getAnnotationElement("synchronized",
                    null, query.getAnnotations());
            if (synchronizedElement != null) {
                if (!("false".equalsIgnoreCase(synchronizedElement.getValue()))) {
                    lockWrapper = new LockWrapper(""); // Query LockWrapper does not need a unique
                    // id since it will
                    // not be passed to the LockSynchronizer.
                    lockWrapper.setLock(new ReentrantLock());   // LockWrapper does not have a default lock
                }
            } else {
                if (isWindow || !(streamRuntime instanceof SingleStreamRuntime)) {
                    if (streamRuntime instanceof JoinStreamRuntime) {
                        // If at least one Window is involved in the join, use the LockWrapper of that window
                        // for the query as well.
                        // If join is between two EventWindows, sync the locks of the LockWrapper of those windows
                        // and use either of them for query.
                        MetaStateEvent metaStateEvent = (MetaStateEvent) streamRuntime.getMetaComplexEvent();
                        MetaStreamEvent[] metaStreamEvents = metaStateEvent.getMetaStreamEvents();

                        if (metaStreamEvents[0].getEventType() == EventType.WINDOW &&
                                metaStreamEvents[1].getEventType() == EventType.WINDOW) {
                            LockWrapper leftLockWrapper = windowMap.get(metaStreamEvents[0]
                                    .getLastInputDefinition().getId()).getLock();
                            LockWrapper rightLockWrapper = windowMap.get(metaStreamEvents[1]
                                    .getLastInputDefinition().getId()).getLock();

                            if (!leftLockWrapper.equals(rightLockWrapper)) {
                                // Sync the lock across both wrappers
                                lockSynchronizer.sync(leftLockWrapper, rightLockWrapper);
                            }
                            // Can use either leftLockWrapper or rightLockWrapper since both of them will hold the
                            // same lock internally
                            // If either of their lock is updated later, the other lock also will be update by the
                            // LockSynchronizer.
                            lockWrapper = leftLockWrapper;
                        } else if (metaStreamEvents[0].getEventType() == EventType.WINDOW) {
                            // Share the same wrapper as the query lock wrapper
                            lockWrapper = windowMap.get(metaStreamEvents[0].getLastInputDefinition().getId())
                                    .getLock();
                        } else if (metaStreamEvents[1].getEventType() == EventType.WINDOW) {
                            // Share the same wrapper as the query lock wrapper
                            lockWrapper = windowMap.get(metaStreamEvents[1].getLastInputDefinition().getId())
                                    .getLock();
                        } else {
                            // Join does not contain any Window
                            lockWrapper = new LockWrapper("");  // Query LockWrapper does not need a unique
                            // id since
                            // it will not be passed to the LockSynchronizer.
                            lockWrapper.setLock(new ReentrantLock());   // LockWrapper does not have a default lock
                        }

                    } else {
                        lockWrapper = new LockWrapper("");
                        lockWrapper.setLock(new ReentrantLock());
                    }
                }
            }

            OutputRateLimiter outputRateLimiter = OutputParser.constructOutputRateLimiter(
                    query.getOutputStream().getId(), query.getOutputRate(),
                    query.getSelector().getGroupByList().size() != 0, isWindow,
                    siddhiQueryContext);
            if (outputRateLimiter instanceof WrappedSnapshotOutputRateLimiter) {
                selector.setBatchingEnabled(false);
            }

            boolean groupBy = !query.getSelector().getGroupByList().isEmpty();

            OutputCallback outputCallback = OutputParser.constructOutputCallback(query.getOutputStream(),
                    streamRuntime.getMetaComplexEvent().getOutputStreamDefinition(), tableMap, windowMap,
                    !(streamRuntime instanceof SingleStreamRuntime) ||
                            groupBy, siddhiQueryContext);

            QueryParserHelper.reduceMetaComplexEvent(streamRuntime.getMetaComplexEvent());
            QueryParserHelper.updateVariablePosition(streamRuntime.getMetaComplexEvent(), executors);
            QueryParserHelper.initStreamRuntime(streamRuntime, streamRuntime.getMetaComplexEvent(), lockWrapper,
                    siddhiQueryContext.getName());

            // Update cache compile selection variable expression executors
            if (streamRuntime instanceof JoinStreamRuntime) {
                streamRuntime.getSingleStreamRuntimes().forEach((singleStreamRuntime -> {
                    Processor processorChain = singleStreamRuntime.getProcessorChain();
                    if (processorChain instanceof JoinProcessor) {
                        CompiledSelection compiledSelection = ((JoinProcessor) processorChain).getCompiledSelection();
                        if (compiledSelection instanceof AbstractQueryableRecordTable.CompiledSelectionWithCache) {
                            List<VariableExpressionExecutor> variableExpressionExecutors =
                                    ((AbstractQueryableRecordTable.CompiledSelectionWithCache) compiledSelection)
                                            .getVariableExpressionExecutorsForQuerySelector();
                            QueryParserHelper.updateVariablePosition(streamRuntime.getMetaComplexEvent(),
                                    variableExpressionExecutors);
                        }
                    }
                }));
            }

            selector.setEventPopulator(StateEventPopulatorFactory.constructEventPopulator(streamRuntime
                    .getMetaComplexEvent()));

            queryRuntime = new QueryRuntimeImpl(query, streamRuntime, selector, outputRateLimiter, outputCallback,
                    streamRuntime.getMetaComplexEvent(), siddhiQueryContext);

            if (outputRateLimiter instanceof WrappedSnapshotOutputRateLimiter) {
                selector.setBatchingEnabled(false);
                ((WrappedSnapshotOutputRateLimiter) outputRateLimiter)
                        .init(streamRuntime.getMetaComplexEvent().getOutputStreamDefinition().getAttributeList().size(),
                                selector.getAttributeProcessorList(), streamRuntime.getMetaComplexEvent());
            }
            outputRateLimiter.init(lockWrapper, groupBy, siddhiQueryContext);

        } catch (DuplicateDefinitionException e) {
            if (nameElement != null) {
                throw new DuplicateDefinitionException(e.getMessageWithOutContext() + ", when creating query " +
                        nameElement.getValue(), e, e.getQueryContextStartIndex(), e.getQueryContextEndIndex(),
                        siddhiAppContext.getName(), siddhiAppContext.getSiddhiAppString());
            } else {
                throw new DuplicateDefinitionException(e.getMessage(), e, e.getQueryContextStartIndex(),
                        e.getQueryContextEndIndex(), siddhiAppContext.getName(), siddhiAppContext.getSiddhiAppString());
            }
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, query, siddhiAppContext);
            throw t;
        }
        return queryRuntime;
    }
}

/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.util.parser;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.input.ProcessStreamReceiver;
import org.wso2.siddhi.core.query.input.stream.StreamRuntime;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.window.EventWindow;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.execution.query.input.stream.*;

import java.util.List;
import java.util.Map;

public class InputStreamParser {

    /**
     * Parse an InputStream returning corresponding StreamRuntime
     *
     * @param inputStream          input stream to be parsed
     * @param executionPlanContext associated siddhi executionPlanContext
     * @param streamDefinitionMap  map containing user given stream definitions
     * @param tableDefinitionMap   table definition map
     * @param eventTableMap        EventTable Map
     * @param executors            List to hold VariableExpressionExecutors to update after query parsing
     * @param latencyTracker       latency tracker
     * @param outputExpectsExpiredEvents  is output expects ExpiredEvents
     * @return StreamRuntime Stream Runtime
     */
    public static StreamRuntime parse(InputStream inputStream, ExecutionPlanContext executionPlanContext,
                                      Map<String, AbstractDefinition> streamDefinitionMap,
                                      Map<String, AbstractDefinition> tableDefinitionMap,
                                      Map<String, AbstractDefinition> windowDefinitionMap,
                                      Map<String, EventTable> eventTableMap, Map<String, EventWindow> eventWindowMap,
                                      List<VariableExpressionExecutor> executors,
                                      LatencyTracker latencyTracker, boolean outputExpectsExpiredEvents,String queryName) {

        if (inputStream instanceof BasicSingleInputStream || inputStream instanceof SingleInputStream) {
            SingleInputStream singleInputStream = (SingleInputStream) inputStream;
            EventWindow eventWindow = eventWindowMap.get(singleInputStream.getStreamId());
            boolean batchProcessingAllowed = eventWindow != null;      // If stream is from window, allow batch processing
            ProcessStreamReceiver processStreamReceiver = new ProcessStreamReceiver(singleInputStream.getStreamId(), latencyTracker,queryName);
            processStreamReceiver.setBatchProcessingAllowed(batchProcessingAllowed);
            return SingleInputStreamParser.parseInputStream((SingleInputStream) inputStream,
                    executionPlanContext, executors, streamDefinitionMap, null, windowDefinitionMap, eventTableMap, new MetaStreamEvent(), processStreamReceiver, true, outputExpectsExpiredEvents,queryName);
        } else if (inputStream instanceof JoinInputStream) {
            return JoinInputStreamParser.parseInputStream(((JoinInputStream) inputStream), executionPlanContext, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, eventTableMap, eventWindowMap, executors, latencyTracker, outputExpectsExpiredEvents,queryName);
        } else if (inputStream instanceof StateInputStream) {
            MetaStateEvent metaStateEvent = new MetaStateEvent(inputStream.getAllStreamIds().size());
            return StateInputStreamParser.parseInputStream(((StateInputStream) inputStream), executionPlanContext,
                    metaStateEvent, streamDefinitionMap, null, null, eventTableMap, executors, latencyTracker,queryName);
        } else {
            throw new OperationNotSupportedException();
        }
    }
}

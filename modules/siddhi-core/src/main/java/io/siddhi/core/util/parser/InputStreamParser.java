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
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.ProcessStreamReceiver;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.table.Table;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.List;
import java.util.Map;

/**
 * Class to parse {@link InputStream}
 */
public class InputStreamParser {

    /**
     * Parse an InputStream returning corresponding StreamRuntime
     *
     * @param inputStream                input stream to be parsed
     * @param streamDefinitionMap        map containing user given stream definitions
     * @param tableDefinitionMap         table definition map
     * @param windowDefinitionMap        window definition map
     * @param aggregationDefinitionMap   aggregation definition map
     * @param tableMap                   Table Map
     * @param windowMap                  event window map
     * @param aggregationMap             aggregator map
     * @param executors                  List to hold VariableExpressionExecutors to update after query parsing
     * @param outputExpectsExpiredEvents is expired events sent as output
     * @param siddhiQueryContext         Siddhi query context.
     * @return StreamRuntime
     */
    public static StreamRuntime parse(InputStream inputStream, Query query,
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

        if (inputStream instanceof BasicSingleInputStream || inputStream instanceof SingleInputStream) {
            SingleInputStream singleInputStream = (SingleInputStream) inputStream;
            ProcessStreamReceiver processStreamReceiver = new ProcessStreamReceiver(singleInputStream.getStreamId(),
                    siddhiQueryContext);
            return SingleInputStreamParser.parseInputStream((SingleInputStream) inputStream,
                    executors, streamDefinitionMap,
                    tableDefinitionMap, windowDefinitionMap, aggregationDefinitionMap, tableMap,
                    new MetaStreamEvent(), processStreamReceiver, true,
                    outputExpectsExpiredEvents, false, false, siddhiQueryContext);
        } else if (inputStream instanceof JoinInputStream) {
            return JoinInputStreamParser.parseInputStream(((JoinInputStream) inputStream),
                    query, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap,
                    aggregationDefinitionMap, tableMap, windowMap, aggregationMap, executors,
                    outputExpectsExpiredEvents, siddhiQueryContext);
        } else if (inputStream instanceof StateInputStream) {
            MetaStateEvent metaStateEvent = new MetaStateEvent(inputStream.getAllStreamIds().size());
            return StateInputStreamParser.parseInputStream(((StateInputStream) inputStream),
                    metaStateEvent, streamDefinitionMap, tableDefinitionMap,
                    windowDefinitionMap, aggregationDefinitionMap, tableMap, executors,
                    siddhiQueryContext);
        } else {
            throw new OperationNotSupportedException();
        }
    }
}

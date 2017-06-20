/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.util.parser.helper;

import org.wso2.siddhi.core.executor.GlobalExpressionExecutor;
import org.wso2.siddhi.core.executor.GlobalVariableExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.input.ProcessStreamReceiver;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.window.Window;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.util.List;
import java.util.Map;

/**
 * Created by gobinath on 6/19/17.
 */
public class ParameterWrapper {

    private Map<String, AbstractDefinition> streamDefinitionMap;
    private Map<String, AbstractDefinition> tableDefinitionMap;
    private Map<String, AbstractDefinition> windowDefinitionMap;
    private Map<String, Table> tableMap;
    private Map<String, Window> eventWindowMap;
    private Map<String, GlobalVariableExpressionExecutor> variableMap;
    //    private Map<String, GlobalExpressionExecutor> expressionMap;
    private Map<String, List<Source>> eventSourceMap;
    private Map<String, List<Sink>> eventSinkMap;
    private List<VariableExpressionExecutor> variableExpressionExecutors;
    private Map<String, ProcessStreamReceiver> processStreamReceiverMap;

    public ParameterWrapper(List<VariableExpressionExecutor> variableExpressionExecutors) {
        this.variableExpressionExecutors = variableExpressionExecutors;
    }

    public ParameterWrapper(Map<String, Table> tableMap, Map<String, Window> eventWindowMap,
                            Map<String, GlobalVariableExpressionExecutor> variableMap) {

        this.tableMap = tableMap;
        this.eventWindowMap = eventWindowMap;
        this.variableMap = variableMap;
    }

    public ParameterWrapper(Map<String, AbstractDefinition> streamDefinitionMap,
                            Map<String, AbstractDefinition> tableDefinitionMap,
                            Map<String, AbstractDefinition> windowDefinitionMap,
                            Map<String, Table> tableMap,
                            Map<String, Window> eventWindowMap,
                            Map<String, GlobalVariableExpressionExecutor> variableMap,
                            Map<String, List<Source>> eventSourceMap,
                            Map<String, List<Sink>> eventSinkMap) {

        this.streamDefinitionMap = streamDefinitionMap;
        this.tableDefinitionMap = tableDefinitionMap;
        this.windowDefinitionMap = windowDefinitionMap;
        this.tableMap = tableMap;
        this.eventWindowMap = eventWindowMap;
        this.variableMap = variableMap;
        this.eventSourceMap = eventSourceMap;
        this.eventSinkMap = eventSinkMap;
    }

    public ParameterWrapper(Map<String, AbstractDefinition> streamDefinitionMap,
                            Map<String, AbstractDefinition> tableDefinitionMap,
                            Map<String, AbstractDefinition> windowDefinitionMap, Map<String, Table> tableMap,
                            Map<String, Window> eventWindowMap,
                            Map<String, GlobalVariableExpressionExecutor> variableMap,
                            Map<String, List<Source>> eventSourceMap,
                            Map<String, List<Sink>> eventSinkMap,
                            Map<String, ProcessStreamReceiver> processStreamReceiverMap) {

        this(streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, tableMap, eventWindowMap, variableMap,
                eventSourceMap, eventSinkMap);
        this.processStreamReceiverMap = processStreamReceiverMap;
    }

    public ParameterWrapper copy() {

        ParameterWrapper parameterWrapper = new ParameterWrapper(this.streamDefinitionMap, this.tableDefinitionMap,
                this.windowDefinitionMap, this.tableMap, this.eventWindowMap, this.variableMap, this.eventSourceMap,
                this.eventSinkMap);
        parameterWrapper.processStreamReceiverMap = this.processStreamReceiverMap;
        parameterWrapper.variableExpressionExecutors = this.variableExpressionExecutors;
        return parameterWrapper;
    }

    public ParameterWrapper variableExpressionExecutors(List<VariableExpressionExecutor> executors) {
        this.variableExpressionExecutors = executors;
        return this;
    }

    public ParameterWrapper processStreamReceiverMap(Map<String, ProcessStreamReceiver> processStreamReceiverMap) {
        this.processStreamReceiverMap = processStreamReceiverMap;
        return this;
    }

    public ParameterWrapper tableDefinitionMap(Map<String, AbstractDefinition> tableDefinitionMap) {
        this.tableDefinitionMap = tableDefinitionMap;
        return this;
    }

    public ParameterWrapper windowDefinitionMap(Map<String, AbstractDefinition> windowDefinitionMap) {
        this.windowDefinitionMap = windowDefinitionMap;
        return this;
    }

    public Map<String, AbstractDefinition> getStreamDefinitionMap() {
        return streamDefinitionMap;
    }

    public Map<String, AbstractDefinition> getTableDefinitionMap() {
        return tableDefinitionMap;
    }

    public Map<String, AbstractDefinition> getWindowDefinitionMap() {
        return windowDefinitionMap;
    }

    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    public Map<String, Window> getEventWindowMap() {
        return eventWindowMap;
    }

    public Map<String, GlobalVariableExpressionExecutor> getVariableMap() {
        return variableMap;
    }

    public Map<String, List<Source>> getEventSourceMap() {
        return eventSourceMap;
    }

    public Map<String, List<Sink>> getEventSinkMap() {
        return eventSinkMap;
    }

    public List<VariableExpressionExecutor> getVariableExpressionExecutors() {
        return variableExpressionExecutors;
    }

    public Map<String, GlobalExpressionExecutor> getExpressionMap() {
        return null;
    }

    public Map<String, ProcessStreamReceiver> getProcessStreamReceiverMap() {
        return processStreamReceiverMap;
    }
}

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
package org.wso2.siddhi.core.query.processor.stream.window;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.table.record.AbstractQueryableRecordTable;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Window operating based on {@link Table}.
 */
public class TableWindowProcessor extends WindowProcessor implements QueryableProcessor {

    private static final Logger log = Logger.getLogger(TableWindowProcessor.class);
    private Table table;
    private boolean isOptimisableLookup;
    private boolean outputExpectsExpiredEvents;
    private ConfigReader configReader;

    public TableWindowProcessor(Table table) {
        this.table = table;
    }


    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.configReader = configReader;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.isOptimisableLookup = table instanceof AbstractQueryableRecordTable;
    }

    public boolean isOptimisableLookup() {
        return isOptimisableLookup;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        // nothing to be done
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return table.find(matchingEvent, compiledCondition);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                               SiddhiAppContext siddhiAppContext,
                                               List<VariableExpressionExecutor> variableExpressionExecutors,
                                               Map<String, Table> tableMap, String queryName) {
        return table.compileCondition(condition, matchingMetaInfoHolder, siddhiAppContext,
                variableExpressionExecutors, tableMap, queryName);
    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        return ((AbstractQueryableRecordTable) this.table).query(matchingEvent, compiledCondition, compiledSelection,
                outputAttributes);    }

    @Override
    public StreamEvent query(StateEvent matchingEvent, CompiledCondition compiledCondition,
                             CompiledSelection compiledSelection) throws ConnectionUnavailableException {
        // Depreciated
        return query(matchingEvent, compiledCondition, compiledSelection, null);
    }

    @Override
    public CompiledSelection compileSelection(Selector selector, List<Attribute> expectedOutputAttributes,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, String queryName) {
        return ((AbstractQueryableRecordTable) this.table).compileSelection(selector, expectedOutputAttributes,
                matchingMetaInfoHolder, siddhiAppContext, variableExpressionExecutors, tableMap, queryName);
    }

    public Table getTable() {
        return table;
    }

    public String getTableId() {
        return table.getTableDefinition().getId();
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Processor cloneProcessor(String key) {
        try {
            TableWindowProcessor streamProcessor = new TableWindowProcessor(table);
            streamProcessor.inputDefinition = inputDefinition;
            ExpressionExecutor[] innerExpressionExecutors = new ExpressionExecutor[attributeExpressionLength];
            ExpressionExecutor[] attributeExpressionExecutors1 = this.attributeExpressionExecutors;
            for (int i = 0; i < attributeExpressionLength; i++) {
                innerExpressionExecutors[i] = attributeExpressionExecutors1[i].cloneExecutor(key);
            }
            streamProcessor.attributeExpressionExecutors = innerExpressionExecutors;
            streamProcessor.attributeExpressionLength = attributeExpressionLength;
            streamProcessor.additionalAttributes = additionalAttributes;
            streamProcessor.complexEventPopulater = complexEventPopulater;
            streamProcessor.init(inputDefinition, attributeExpressionExecutors, configReader, siddhiAppContext,
                    outputExpectsExpiredEvents);
            streamProcessor.start();
            return streamProcessor;

        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception in cloning " + this.getClass().getCanonicalName(), e);
        }
    }

    @Override
    public Map<String, Object> currentState() {
        //No state
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //Nothing to be done
    }
}

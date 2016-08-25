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

package org.wso2.siddhi.core.table;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.table.holder.EventHolder;
import org.wso2.siddhi.core.util.collection.OverwritingStreamEventExtractor;
import org.wso2.siddhi.core.util.collection.UpdateAttributeMapper;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.parser.EventHolderPasser;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory event table implementation of SiddhiQL.
 */
public class InMemoryEventTable implements EventTable, Snapshotable {

    private TableDefinition tableDefinition;
    private StreamEventCloner tableStreamEventCloner;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private EventHolder eventHolder;
    private String elementId;


    @Override
    public void init(TableDefinition tableDefinition, MetaStreamEvent tableMetaStreamEvent,
                     StreamEventPool tableStreamEventPool, StreamEventCloner tableStreamEventCloner,
                     ExecutionPlanContext executionPlanContext) {
        this.tableDefinition = tableDefinition;
        this.tableStreamEventCloner = tableStreamEventCloner;

        eventHolder = EventHolderPasser.parse(tableDefinition, tableStreamEventPool);

        if (elementId == null) {
            elementId = "InMemoryEventTable"+ executionPlanContext.getElementIdGenerator().createNewId();
        }
        executionPlanContext.getSnapshotService().addSnapshotable(tableDefinition.getId(), this);
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        try {
            readWriteLock.writeLock().lock();
            eventHolder.add(addingEventChunk);
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, Operator operator) {
        try {
            readWriteLock.writeLock().lock();
            operator.delete(deletingEventChunk, eventHolder);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, Operator operator,
                       UpdateAttributeMapper[] updateAttributeMappers) {
        try {
            readWriteLock.writeLock().lock();
            operator.update(updatingEventChunk, eventHolder, updateAttributeMappers);
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    @Override
    public void overwriteOrAdd(ComplexEventChunk<StateEvent> overwritingOrAddingEventChunk, Operator operator,
                               UpdateAttributeMapper[] updateAttributeMappers,
                               OverwritingStreamEventExtractor overwritingStreamEventExtractor) {
        try {
            readWriteLock.writeLock().lock();
            ComplexEventChunk<StreamEvent> failedEvents = operator.overwriteOrAdd(overwritingOrAddingEventChunk,
                    eventHolder, updateAttributeMappers, overwritingStreamEventExtractor);
            eventHolder.add(failedEvents);
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    @Override
    public boolean contains(StateEvent matchingEvent, Finder finder) {
        try {
            readWriteLock.readLock().lock();
            return finder.contains(matchingEvent, eventHolder);
        } finally {
            readWriteLock.readLock().unlock();
        }

    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, Finder finder) {
        try {
            readWriteLock.readLock().lock();
            return finder.find(matchingEvent, eventHolder, tableStreamEventCloner);
        } finally {
            readWriteLock.readLock().unlock();
        }

    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(eventHolder, expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap,tableDefinition.getId());
    }


    @Override
    public Operator constructOperator(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                      ExecutionPlanContext executionPlanContext,
                                      List<VariableExpressionExecutor> variableExpressionExecutors,
                                      Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(eventHolder, expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap,tableDefinition.getId());
    }


    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("EventHolder", eventHolder)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        eventHolder = (EventHolder) stateEntry.getValue();
    }

    @Override
    public String getElementId() {
        return elementId;
    }
}

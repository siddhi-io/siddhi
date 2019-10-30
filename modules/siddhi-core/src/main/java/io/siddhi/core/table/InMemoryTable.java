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

package io.siddhi.core.table;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.holder.EventHolder;
import io.siddhi.core.table.record.RecordTableHandler;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.EventHolderPasser;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory event table implementation of SiddhiQL.
 */
public class InMemoryTable extends Table {
    StreamEventCloner tableStreamEventCloner;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    StateHolder<TableState> stateHolder;

    @Override
    public void init(TableDefinition tableDefinition, StreamEventFactory storeEventPool,
                     StreamEventCloner storeEventCloner, ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                     RecordTableHandler recordTableHandler) {
        this.tableDefinition = tableDefinition;
        this.tableStreamEventCloner = storeEventCloner;
        EventHolder eventHolder = EventHolderPasser.parse(tableDefinition, storeEventPool, siddhiAppContext, false);

        stateHolder = siddhiAppContext.generateStateHolder(tableDefinition.getId(),
                () -> new TableState(eventHolder));
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            state.eventHolder.add(addingEventChunk);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }

    }

    @Override
    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            ((Operator) ((InMemoryCompiledCondition) compiledCondition).getOperatorCompiledCondition()).
                    delete(deletingEventChunk, state.eventHolder);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        try {
            ((Operator) ((InMemoryCompiledCondition) compiledCondition).getOperatorCompiledCondition()).
                    update(updatingEventChunk, state.eventHolder, (InMemoryCompiledUpdateSet) compiledUpdateSet);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }

    }

    @Override
    public void updateOrAdd(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                            CompiledCondition compiledCondition,
                            CompiledUpdateSet compiledUpdateSet,
                            AddingStreamEventExtractor addingStreamEventExtractor) {
        readWriteLock.writeLock().lock();
        TableState state = stateHolder.getState();
        InMemoryCompiledCondition inMemoryCompiledCondition = (InMemoryCompiledCondition) compiledCondition;
        try {
            ComplexEventChunk<StateEvent> failedEvents =
                    ((Operator) inMemoryCompiledCondition.getOperatorCompiledCondition()).
                            tryUpdate(updateOrAddingEventChunk, state.eventHolder,
                                    (InMemoryCompiledUpdateSet) compiledUpdateSet,
                                    addingStreamEventExtractor);
            if (failedEvents != null && failedEvents.getFirst() != null) {
                state.eventHolder.add(reduceEventsForUpdateOrInsert(
                        addingStreamEventExtractor, inMemoryCompiledCondition,
                        (InMemoryCompiledUpdateSet) compiledUpdateSet, failedEvents));
            }
        } finally {
            stateHolder.returnState(state);
            readWriteLock.writeLock().unlock();
        }
    }

    protected ComplexEventChunk<StreamEvent> reduceEventsForUpdateOrInsert(
            AddingStreamEventExtractor addingStreamEventExtractor,
            InMemoryCompiledCondition inMemoryCompiledCondition,
            InMemoryCompiledUpdateSet compiledUpdateSet, ComplexEventChunk<StateEvent> failedEvents) {
        ComplexEventChunk<StreamEvent> toInsertEventChunk = new ComplexEventChunk<>();
        failedEvents.reset();
        while (failedEvents.hasNext()) {
            StateEvent failedEvent = failedEvents.next();
            boolean updated = false;
            toInsertEventChunk.reset();
            while (toInsertEventChunk.hasNext()) {
                StreamEvent toInsertEvent = toInsertEventChunk.next();
                failedEvent.setEvent(inMemoryCompiledCondition.getStoreEventIndex(), toInsertEvent);
                if ((Boolean) inMemoryCompiledCondition.
                        getUpdateOrInsertExpressionExecutor().execute(failedEvent)) {
                    for (Map.Entry<Integer, ExpressionExecutor> entry :
                            compiledUpdateSet.getExpressionExecutorMap().entrySet()) {
                        toInsertEvent.setOutputData(entry.getValue().
                                execute(failedEvent), entry.getKey());
                    }
                    updated = true;
                }
            }
            if (!updated) {
                toInsertEventChunk.add(addingStreamEventExtractor.getAddingStreamEvent(failedEvent));
            }
        }
        return toInsertEventChunk;
    }

    @Override
    public boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        readWriteLock.readLock().lock();
        TableState state = stateHolder.getState();
        try {
            return ((Operator) ((InMemoryCompiledCondition) compiledCondition).getOperatorCompiledCondition()).
                    contains(matchingEvent, state.eventHolder);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    protected void disconnect() {

    }

    @Override
    protected void destroy() {

    }

    @Override
    public StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        TableState state = stateHolder.getState();
        readWriteLock.readLock().lock();
        try {
            return ((Operator) ((InMemoryCompiledCondition) compiledCondition).getOperatorCompiledCondition()).
                    find(matchingEvent, state.eventHolder, tableStreamEventCloner);
        } finally {
            stateHolder.returnState(state);
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        TableState state = stateHolder.getState();
        try {
            return new InMemoryCompiledCondition(OperatorParser.constructOperator(state.eventHolder, condition,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext),
                    ExpressionParser.parseExpression(condition, matchingMetaInfoHolder.getMetaStateEvent(),
                            matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
                            false, 0, ProcessingMode.BATCH,
                            false, siddhiQueryContext),
                    matchingMetaInfoHolder.getStoreEventIndex()
            );
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public CompiledUpdateSet compileUpdateSet(UpdateSet updateSet, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        Map<Integer, ExpressionExecutor> expressionExecutorMap = new HashMap<>();
        for (UpdateSet.SetAttribute setAttribute : updateSet.getSetAttributeList()) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(
                    setAttribute.getAssignmentExpression(), matchingMetaInfoHolder.getMetaStateEvent(),
                    matchingMetaInfoHolder.getCurrentState(), tableMap, variableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH, false,
                    siddhiQueryContext);
            int attributePosition = tableDefinition.
                    getAttributePosition(setAttribute.getTableVariable().getAttributeName());
            expressionExecutorMap.put(attributePosition, expressionExecutor);
        }
        return new InMemoryCompiledUpdateSet(expressionExecutorMap);
    }

    @Override
    protected void connectAndLoadCache() throws ConnectionUnavailableException {

    }

    public int size() {
        return stateHolder.getState().eventHolder.size();
    }

    public boolean isStateful() {
        return true;
    }

    /**
     * class to store the state of table
     */
    public class TableState extends State {
        private final EventHolder eventHolder;

        public TableState(EventHolder eventHolder) {
            this.eventHolder = eventHolder;
        }

        public EventHolder getEventHolder() {
            return eventHolder;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("EventHolder", eventHolder.getSnapshot());
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            eventHolder.restore((SnapshotStateList) state.get("EventHolder"));
        }
    }
}

/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.core.table.record;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RecordTableHandler is an optional handler that can be implemented to do processing on output events before sending
 * to the AbstractRecordTable.
 *
 * @param <S> current state
 */
public abstract class RecordTableHandler<S extends State> {

    private String id;
    private RecordTableHandlerCallback recordTableHandlerCallback;
    private StateHolder<S> stateHolder;

    protected final void init(TableDefinition tableDefinition, RecordTableHandlerCallback recordTableHandlerCallback,
                              SiddhiAppContext siddhiAppContext) {
        this.recordTableHandlerCallback = recordTableHandlerCallback;

        id = siddhiAppContext.getName() + "-" + tableDefinition.getId() + "-" + this.getClass().getName();
        StateFactory<S> stateFactory = init(id, tableDefinition);
        stateHolder = siddhiAppContext.generateStateHolder(
                tableDefinition.getId() + "-" + this.getClass().getName(),
                stateFactory);

    }

    public String getId() {
        return id;
    }

    /**
     * Initialize the Record Table Handler
     *
     * @param id              is the generated id for the record table handler
     * @param tableDefinition is the definition of the table with annotations if any
     */
    public abstract StateFactory<S> init(String id, TableDefinition tableDefinition);

    public void add(long timestamp, List<Object[]> records) throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            add(timestamp, records, recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                  the timestamp of the last event in the event chunk
     * @param records                    records that need to be added to the table, each Object[] represent a
     *                                   record and it will match the attributes of the Table Definition.
     * @param recordTableHandlerCallback call back to do operations on the record table
     * @param state                      current state
     * @throws ConnectionUnavailableException
     */
    public abstract void add(long timestamp, List<Object[]> records,
                             RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

    public void delete(long timestamp, List<Map<String, Object>> deleteConditionParameterMaps,
                       CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            delete(timestamp, deleteConditionParameterMaps, compiledCondition, recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                    the timestamp of the last event in the event chunk
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @param recordTableHandlerCallback   call back to do operations on the record table
     * @param state                        current state
     * @throws ConnectionUnavailableException
     */
    public abstract void delete(long timestamp, List<Map<String, Object>> deleteConditionParameterMaps,
                                CompiledCondition compiledCondition,
                                RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

    public void update(long timestamp, CompiledCondition compiledCondition,
                       List<Map<String, Object>> updateConditionParameterMaps,
                       LinkedHashMap<String, CompiledExpression> updateSetMap,
                       List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            update(timestamp, compiledCondition, updateConditionParameterMaps, updateSetMap, updateSetParameterMaps,
                    recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                    the timestamp of the last event in the event chunk
     * @param updateCondition              the compiledCondition against which records should be matched for update
     * @param updateConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition based on which the records will be updated
     * @param updateSetExpressions         the set of updates mappings and related complied expressions
     * @param updateSetParameterMaps       map of matching StreamVariable Ids and their values corresponding to the
     * @param recordTableHandlerCallback   call back to do operations on the record table
     * @param state                        current state
     * @throws ConnectionUnavailableException
     */
    public abstract void update(long timestamp, CompiledCondition updateCondition,
                                List<Map<String, Object>> updateConditionParameterMaps,
                                LinkedHashMap<String, CompiledExpression> updateSetExpressions,
                                List<Map<String, Object>> updateSetParameterMaps,
                                RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

    public void updateOrAdd(long timestamp, CompiledCondition compiledCondition,
                            List<Map<String, Object>> updateConditionParameterMaps,
                            LinkedHashMap<String, CompiledExpression> updateSetMap,
                            List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        updateOrAdd(timestamp, compiledCondition, updateConditionParameterMaps, updateSetMap, updateSetParameterMaps,
                addingRecords, recordTableHandlerCallback);
    }

    /**
     * @param timestamp                    the timestamp of the last event in the event chunk
     * @param updateCondition              the compiledCondition against which records should be matched for update
     * @param updateConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition based on which the records will be updated
     * @param updateSetExpressions         the set of updates mappings and related complied expressions
     * @param updateSetParameterMaps       map of matching StreamVariable Ids and their values corresponding to the
     *                                     update set
     * @param addingRecords                the values for adding new records if the update condition did not match
     * @param recordTableHandlerCallback   call back to do operations on the record table
     * @throws ConnectionUnavailableException
     */
    public abstract void updateOrAdd(long timestamp, CompiledCondition updateCondition,
                                     List<Map<String, Object>> updateConditionParameterMaps,
                                     LinkedHashMap<String, CompiledExpression> updateSetExpressions,
                                     List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords,
                                     RecordTableHandlerCallback recordTableHandlerCallback)
            throws ConnectionUnavailableException;

    public Iterator<Object[]> find(long timestamp, Map<String, Object> findConditionParameterMap,
                                   CompiledCondition compiledCondition) throws
            ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            return find(timestamp, findConditionParameterMap, compiledCondition, recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                  the timestamp of the event used to match from record table
     * @param findConditionParameterMap  map of matching StreamVariable Ids and their values
     *                                   corresponding to the compiled condition
     * @param compiledCondition          the compiledCondition against which records should be matched
     * @param recordTableHandlerCallback call back to do operations on the record table
     * @param state
     * @return RecordIterator of matching records
     * @throws ConnectionUnavailableException
     */
    public abstract Iterator<Object[]> find(long timestamp, Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition,
                                            RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

    public boolean contains(long timestamp, Map<String, Object> containsConditionParameterMap,
                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            return contains(timestamp, containsConditionParameterMap, compiledCondition,
                    recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                     the timestamp of the event used to match from record table
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @param recordTableHandlerCallback    call back to do operations on the record table
     * @param state                         current state
     * @return if matching record found or not
     * @throws ConnectionUnavailableException
     */
    public abstract boolean contains(long timestamp, Map<String, Object> containsConditionParameterMap,
                                     CompiledCondition compiledCondition,
                                     RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;


    @Deprecated
    public Iterator<Object[]> query(long timestamp, Map<String, Object> parameterMap,
                                    CompiledCondition compiledCondition, CompiledSelection compiledSelection)
            throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            return query(timestamp, parameterMap, compiledCondition, compiledSelection, recordTableHandlerCallback,
                    state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    public Iterator<Object[]> query(long timestamp, Map<String, Object> parameterMap,
                                    CompiledCondition compiledCondition, CompiledSelection compiledSelection,
                                    Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        S state = stateHolder.getState();
        try {
            return query(timestamp, parameterMap, compiledCondition, compiledSelection, outputAttributes,
                    recordTableHandlerCallback, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * @param timestamp                  the timestamp of the event used to match from record table
     * @param parameterMap               map of matching StreamVariable Ids and their values
     *                                   corresponding to the compiled condition and selection
     * @param compiledCondition          the compiledCondition against which records should be matched
     * @param compiledSelection          the compiledSelection which maps the events based on selection
     * @param recordTableHandlerCallback call back to do operations on the record table
     * @param state                      current state
     * @return RecordIterator of matching records
     * @throws ConnectionUnavailableException
     */
    @Deprecated
    public abstract Iterator<Object[]> query(long timestamp, Map<String, Object> parameterMap,
                                             CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection,
                                             RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

    public abstract Iterator<Object[]> query(long timestamp, Map<String, Object> parameterMap,
                                             CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection,
                                             Attribute[] outputAttributes,
                                             RecordTableHandlerCallback recordTableHandlerCallback, S state)
            throws ConnectionUnavailableException;

}

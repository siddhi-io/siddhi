/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.stream.input;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TableInputHandler is the {@link Event} entry point to Siddhi. Users can create an Input Handler and then use that to
 * directly inject events inside Siddhi tables.
 */
public class TableInputHandler {

    private static final Logger log = LogManager.getLogger(TableInputHandler.class);

    protected SiddhiAppContext siddhiAppContext;
    protected Table table;

    public TableInputHandler(Table table, SiddhiAppContext siddhiAppContext) {
        this.table = table;
        this.siddhiAppContext = siddhiAppContext;
    }

    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        table.add(addingEventChunk);
    }

    public void delete(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition) {
        table.delete(deletingEventChunk, compiledCondition);
    }

    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                       CompiledUpdateSet compiledUpdateSet) {
        table.update(updatingEventChunk, compiledCondition, compiledUpdateSet);
    }

    public void updateOrAdd(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                            CompiledUpdateSet compiledUpdateSet,
                            AddingStreamEventExtractor addingStreamEventExtractor) {
        table.updateOrAdd(updatingEventChunk, compiledCondition, compiledUpdateSet, addingStreamEventExtractor);
    }
}

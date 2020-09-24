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

package io.siddhi.core.stream.input;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.DefinitionNotExistException;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.table.Table;
import io.siddhi.query.api.definition.AbstractDefinition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Manager class to handle {@link Event} insertion to Siddhi.
 */
public class InputManager {

    private final InputEntryValve inputEntryValve;
    private final SiddhiAppContext siddhiAppContext;
    private Map<String, InputHandler> inputHandlerMap = new LinkedHashMap<String, InputHandler>();
    private Map<String, TableInputHandler> tableInputHandlerMap = new LinkedHashMap<String, TableInputHandler>();
    private Map<String, StreamJunction> streamJunctionMap;
    private ConcurrentMap<String, Table> tableMap;
    private InputDistributor inputDistributor;
    private boolean isConnected = false;

    public InputManager(SiddhiAppContext siddhiAppContext,
                        ConcurrentMap<String, AbstractDefinition> streamDefinitionMap,
                        ConcurrentMap<String, StreamJunction> streamJunctionMap,
                        ConcurrentMap<String, Table> tableMap) {
        this.streamJunctionMap = streamJunctionMap;
        this.tableMap = tableMap;
        this.inputDistributor = new InputDistributor();
        this.inputEntryValve = new InputEntryValve(siddhiAppContext, inputDistributor);
        this.siddhiAppContext = siddhiAppContext;
    }

    public InputHandler getInputHandler(String streamId) {
        InputHandler inputHandler = inputHandlerMap.get(streamId);
        if (inputHandler == null) {
            synchronized (this) {
                inputHandler = inputHandlerMap.get(streamId);
                if (inputHandler == null) {
                    InputHandler newInputHandler = constructInputHandler(streamId);
                    if (this.isConnected) {
                        newInputHandler.connect();
                    }
                    return newInputHandler;
                } else {
                    return inputHandler;
                }
            }
        } else {
            return inputHandler;
        }
    }

    public TableInputHandler getTableInputHandler(String tableId) {
        TableInputHandler tableInputHandler = tableInputHandlerMap.get(tableId);
        synchronized (this) {
            if (tableInputHandler == null) {
                return constructTableInputHandler(tableId);
            }
            return tableInputHandler;
        }
    }

    public synchronized void connect() {
        for (InputHandler inputHandler : inputHandlerMap.values()) {
            inputHandler.connect();
        }
        this.isConnected = true;
    }

    public synchronized void disconnect() {
        for (InputHandler inputHandler : inputHandlerMap.values()) {
            inputHandler.disconnect();
        }
        inputDistributor.clear();
        inputHandlerMap.clear();
        this.isConnected = false;
    }

    public synchronized InputHandler constructInputHandler(String streamId) {
        InputHandler inputHandler = new InputHandler(streamId, inputHandlerMap.size(),
                inputEntryValve, siddhiAppContext);
        StreamJunction streamJunction = streamJunctionMap.get(streamId);
        if (streamJunction == null) {
            throw new DefinitionNotExistException("Stream with stream ID " + streamId + " has not been defined");
        }
        inputDistributor.addInputProcessor(streamJunctionMap.get(streamId).constructPublisher());
        inputHandlerMap.put(streamId, inputHandler);
        return inputHandler;
    }

    public synchronized TableInputHandler constructTableInputHandler(String tableId) {
        Table correspondingTable = tableMap.get(tableId);
        if (correspondingTable == null) {
            throw new DefinitionNotExistException("Table with table ID " + tableId + " has not been defined");
        }
        TableInputHandler tableInputHandler = new TableInputHandler(correspondingTable, siddhiAppContext);
        tableInputHandlerMap.put(tableId, tableInputHandler);
        return tableInputHandler;
    }
}

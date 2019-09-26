/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.snapshot.state;

import java.util.Map;

/**
 * State holder for  partitioned use case
 */
public class PartitionSyncStateHolder implements StateHolder {
    private PartitionStateHolder partitionStateHolder;

    public PartitionSyncStateHolder(StateFactory stateFactory) {
        partitionStateHolder = new PartitionStateHolder(stateFactory);
    }

    @Override
    public synchronized State getState() {
        State state = partitionStateHolder.getState();
        state.increaseActiveUseCount();
        return state;
    }

    @Override
    public synchronized void returnState(State state) {
        state.decreaseActiveUseCount();
        partitionStateHolder.returnState(state);
    }


    @Override
    public synchronized Map<String, Map<String, State>> getAllStates() {
        Map<String, Map<String, State>> states = partitionStateHolder.getAllStates();
        for (Map<String, State> groupByStates : states.values()) {
            for (State state : groupByStates.values()) {
                state.increaseActiveUseCount();
            }
        }
        return states;
    }

    @Override
    public synchronized Map<String, State> getAllGroupByStates() {
        Map<String, State> groupByStates = partitionStateHolder.getAllGroupByStates();
        for (State state : groupByStates.values()) {
            state.increaseActiveUseCount();
        }
        return groupByStates;
    }

    @Override
    public synchronized State cleanGroupByStates() {
        return partitionStateHolder.cleanGroupByStates();
    }

    @Override
    public synchronized void returnGroupByStates(Map states) {
        for (State state : ((Map<String, State>) states).values()) {
            state.decreaseActiveUseCount();
        }
        partitionStateHolder.returnGroupByStates(states);
    }

    @Override
    public synchronized void returnAllStates(Map states) {
        for (Map<String, State> groupByStates : ((Map<String, Map<String, State>>) states).values()) {
            for (State state : groupByStates.values()) {
                state.decreaseActiveUseCount();
            }
        }
        partitionStateHolder.returnAllStates(states);
    }
}

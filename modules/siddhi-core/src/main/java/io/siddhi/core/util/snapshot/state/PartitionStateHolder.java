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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * State holder for  partitioned use case
 */
public class PartitionStateHolder implements StateHolder {
    private static final Logger log = Logger.getLogger(PartitionStateHolder.class);

    private StateFactory stateFactory;
    private Map<String, Map<String, State>> states = new HashMap<>();

    public PartitionStateHolder(StateFactory stateFactory) {
        this.stateFactory = stateFactory;
    }

    @Override
    public synchronized State getState() {
        String partitionFlowId = SiddhiAppContext.getPartitionFlowId();
        String groupByFlowId = SiddhiAppContext.getGroupByFlowId();
        Map<String, State> partitionStates = states.computeIfAbsent(partitionFlowId, k -> new HashMap<>());
        State state = partitionStates.computeIfAbsent(groupByFlowId, s -> stateFactory.createNewState());
        state.activeUseCount++;
        return state;
    }

    @Override
    public synchronized void returnState(State state) {
        String partitionFlowId = SiddhiAppContext.getPartitionFlowId();
        String groupByFlowId = SiddhiAppContext.getGroupByFlowId();
        state.activeUseCount--;
        if (state.activeUseCount == 0) {
            try {
                if (state.canDestroy()) {
                    removeState(partitionFlowId, groupByFlowId);
                }
            } catch (Throwable t) {
                log.error("Dropping partition state for partition key '" + partitionFlowId +
                        "' and the group by key '" + groupByFlowId + "', due to error! " + t.getMessage(), t);
                removeState(partitionFlowId, groupByFlowId);
            }
        } else if (state.activeUseCount < 0) {
            throw new SiddhiAppRuntimeException("State active count has reached less then zero for partition key '" +
                    partitionFlowId + "' and the group by key '" + groupByFlowId + "', current value is " +
                    state.activeUseCount);
        }
    }

    private void removeState(String partitionFlowId, String groupByFlowId) {
        Map<String, State> groupByStates = states.get(partitionFlowId);
        if (groupByStates != null) {
            groupByStates.remove(groupByFlowId);
            if (groupByStates.isEmpty()) {
                states.remove(partitionFlowId);
            }
        }
    }

    public synchronized Map<String, Map<String, State>> getAllStates() {
        for (Map<String, State> groupByStates : states.values()) {
            for (State state : groupByStates.values()) {
                state.activeUseCount++;
            }
        }
        return states;
    }

    @Override
    public synchronized Map<String, State> getAllGroupByStates() {
        String partitionFlowId = SiddhiAppContext.getPartitionFlowId();
        Map<String, State> groupByStates = states.computeIfAbsent(partitionFlowId, k -> new HashMap<>());
        for (State state : groupByStates.values()) {
            state.activeUseCount++;
        }
        return groupByStates;
    }

    @Override
    public synchronized void returnGroupByStates(Map states) {
        String partitionFlowId = SiddhiAppContext.getPartitionFlowId();
        for (Iterator<Map.Entry<String, State>> iterator =
             ((Set<Map.Entry<String, State>>) states.entrySet()).iterator();
             iterator.hasNext(); ) {
            Map.Entry<String, State> stateEntry = iterator.next();
            State state = stateEntry.getValue();
            state.activeUseCount--;
            if (state.activeUseCount == 0) {
                try {
                    if (state.canDestroy()) {
                        iterator.remove();
                    }
                } catch (Throwable t) {
                    log.error("Dropping partition state for partition key '" + partitionFlowId +
                            "' and the group by key '" + stateEntry.getKey() + "', due to error! " + t.getMessage(), t);
                    iterator.remove();
                }
            } else if (state.activeUseCount < 0) {
                throw new SiddhiAppRuntimeException("State active count has reached less then zero for partition key '"
                        + partitionFlowId + "' and the group by key '" + stateEntry.getKey() + "', current value is " +
                        state.activeUseCount);
            }

        }
        if (states.isEmpty()) {
            states.remove(partitionFlowId);
        }
    }

    @Override
    public synchronized void returnAllStates(Map states) {
        for (Iterator<Map.Entry<String, Map<String, State>>> statesIterator =
             ((Set<Map.Entry<String, Map<String, State>>>) states.entrySet()).iterator(); statesIterator.hasNext(); ) {
            Map.Entry<String, Map<String, State>> statesEntry = statesIterator.next();
            for (Iterator<Map.Entry<String, State>> stateIterator = statesEntry.getValue().entrySet().iterator();
                 stateIterator.hasNext(); ) {
                Map.Entry<String, State> stateEntry = stateIterator.next();
                State state = stateEntry.getValue();
                state.activeUseCount--;
                if (state.activeUseCount == 0) {
                    try {
                        if (state.canDestroy()) {
                            stateIterator.remove();
                        }
                    } catch (Throwable t) {
                        log.error("Dropping partition state for partition key '" + statesEntry.getKey() +
                                "' and the group by key '" + stateEntry.getKey() + "', due to error! " +
                                t.getMessage(), t);
                        stateIterator.remove();
                    }
                } else if (state.activeUseCount < 0) {
                    throw new SiddhiAppRuntimeException("State active count has reached less then zero for " +
                            "partition key '" + statesEntry.getKey() + "' and the group by key '" +
                            stateEntry.getKey() + "', current value is " +
                            state.activeUseCount);
                }
            }
            if (statesEntry.getValue().isEmpty()) {
                statesIterator.remove();
            }
        }
    }


}

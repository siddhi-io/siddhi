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
import java.util.Map;
import java.util.Set;

/**
 * State holder for  partitioned use case
 */
public class PartitionStateHolder implements StateHolder {
    private static final Logger log = Logger.getLogger(PartitionStateHolder.class);

    private StateFactory stateFactory;
    private Map<String, State> states = new HashMap<>();

    public PartitionStateHolder(StateFactory stateFactory) {
        this.stateFactory = stateFactory;
    }

    @Override
    public synchronized State getState() {
        String key = SiddhiAppContext.getCurrentFlowId();
        State state = states.get(key);
        if (state == null) {
            state = stateFactory.createNewState();
            states.put(key, state);
        }
        state.activeUseCount++;
        return state;
    }

    @Override
    public synchronized void returnState(State state) {
        String key = SiddhiAppContext.getCurrentFlowId();
        state.activeUseCount--;
        if (state.activeUseCount < 0) {
            throw new SiddhiAppRuntimeException("State active count has reached less then zero for partition key '" +
                    key + "', current value is " + state.activeUseCount);
        }
        try {

            if (state.activeUseCount == 0 && state.canDestroy()) {
                states.remove(key);
            }
        } catch (Throwable t) {
            log.error("Dropping partition state '" + key + "' due to error! " + t.getMessage(), t);
            states.remove(key);
        }
    }

    public synchronized Map<String, State> getAllStates() {
        for (Map.Entry<String, State> entry : states.entrySet()) {
            entry.getValue().activeUseCount++;
        }
        return new HashMap<>(states);
    }

    @Override
    public synchronized void returnStates(Map states) {
        for (Map.Entry<String, State> entry : (Set<Map.Entry<String, State>>) states.entrySet()) {
            entry.getValue().activeUseCount--;
            if (entry.getValue().activeUseCount < 0) {
                throw new SiddhiAppRuntimeException("State active count has reached less then zero for partition key '"
                        + entry.getKey() + "', current value is " + entry.getValue().activeUseCount);
            }
            String key = entry.getKey();
            try {
                if (entry.getValue().activeUseCount == 0 && entry.getValue().canDestroy()) {
                    this.states.remove(key);
                }
            } catch (Throwable t) {
                log.error("Dropping partition state '" + key + "' due to error! " + t.getMessage(), t);
                this.states.remove(key);
            }

        }
    }


}

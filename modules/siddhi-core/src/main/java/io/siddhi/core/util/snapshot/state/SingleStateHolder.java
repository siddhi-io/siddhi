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

import io.siddhi.core.exception.SiddhiAppRuntimeException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * State holder for non partition use case
 */
public class SingleStateHolder implements StateHolder {
    private static final Logger log = Logger.getLogger(SingleStateHolder.class);

    private final StateFactory stateFactory;
    private State state = null;

    public SingleStateHolder(StateFactory stateFactory) {
        this.stateFactory = stateFactory;
    }

    @Override
    public synchronized State getState() {
        if (state == null) {
            state = stateFactory.createNewState();
        }
        state.activeUseCount++;
        return state;
    }

    @Override
    public synchronized void returnState(State state) {
        state.activeUseCount--;
        if (state.activeUseCount < 0) {
            throw new SiddhiAppRuntimeException("State active count has reached less then zero, current value is "
                    + state.activeUseCount);
        }
        try {
            if (state.activeUseCount == 0 && state.canDestroy()) {
                this.state = null;
            }
        } catch (Throwable t) {
            log.error("Dropping state due to error! " + t.getMessage(), t);
            this.state = null;
        }

    }

    public synchronized Map<String, State> getAllStates() {
        if (state == null) {
            state = stateFactory.createNewState();
        }
        Map<String, State> result = new HashMap<>(1);
        result.put(null, state);
        state.activeUseCount++;
        return result;
    }

    @Override
    public synchronized void returnStates(Map states) {
        for (Map.Entry<String, State> entry : (Set<Map.Entry<String, State>>) states.entrySet()) {
            entry.getValue().activeUseCount--;
            if (entry.getValue().activeUseCount < 0) {
                throw new SiddhiAppRuntimeException("State active count has reached less then zero, current value is "
                        + entry.getValue().activeUseCount);
            }
            try {
                if (entry.getValue().activeUseCount == 0 && entry.getValue().canDestroy()) {
                    this.state = null;
                }
            } catch (Throwable t) {
                log.error("Dropping state due to error! " + t.getMessage(), t);
                this.state = null;
            }

        }
    }
}

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * State holder for non partition use case
 */
public class SingleStateHolder implements StateHolder {
    private static final Logger log = LogManager.getLogger(SingleStateHolder.class);
    final Map<String, State> groupByStates = new HashMap<>(1);
    final Map<String, Map<String, State>> allStates = new HashMap<>(1);
    private final StateFactory stateFactory;
    private State state = null;

    public SingleStateHolder(StateFactory stateFactory) {
        this.stateFactory = stateFactory;
        this.allStates.put(null, groupByStates);
    }

    @Override
    public State getState() {
        if (state == null) {
            state = stateFactory.createNewState();
            groupByStates.put(null, state);
        }
        return state;
    }

    @Override
    public void returnState(State state) {
        //ignore
    }

    public Map<String, Map<String, State>> getAllStates() {
        if (state == null) {
            state = stateFactory.createNewState();
            groupByStates.put(null, state);
        }
        return allStates;
    }

    @Override
    public void returnAllStates(Map states) {
        //ignore

    }

    @Override
    public Map<String, State> getAllGroupByStates() {
        if (state == null) {
            state = stateFactory.createNewState();
            groupByStates.put(null, state);
        }
        return groupByStates;
    }

    @Override
    public State cleanGroupByStates() {
        State returnValue = state;
        state = null;
        groupByStates.clear();
        return returnValue;
    }

    @Override
    public void returnGroupByStates(Map states) {
        //ignore
    }
}

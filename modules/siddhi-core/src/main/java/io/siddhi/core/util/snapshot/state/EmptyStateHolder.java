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

import java.util.HashMap;
import java.util.Map;

/**
 * State holder for non partition use case
 */
public class EmptyStateHolder implements StateHolder {

    private Map emptyMap = new HashMap(0);

    @Override
    public State getState() {
        return null;
    }

    @Override
    public void returnState(State state) {
    }

    @Override
    public Map<String, State> getAllStates() {
        return emptyMap;
    }

    @Override
    public void returnAllStates(Map partitionKeyStates) {

    }

    @Override
    public Map getAllGroupByStates() {
        return emptyMap;
    }

    @Override
    public State cleanGroupByStates() {
        return null;
    }

    @Override
    public void returnGroupByStates(Map states) {

    }
}

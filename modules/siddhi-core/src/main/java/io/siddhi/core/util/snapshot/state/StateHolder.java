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
 * Holder to have all the states
 *
 * @param <S> state
 */
public interface StateHolder<S extends State> {

    S getState();

    void returnState(S state);

    Map<String, Map<String, S>> getAllStates();

    void returnAllStates(Map<String, Map<String, S>> states);

    Map<String, S> getAllGroupByStates();

    S cleanGroupByStates();

    void returnGroupByStates(Map<String, S> states);
}

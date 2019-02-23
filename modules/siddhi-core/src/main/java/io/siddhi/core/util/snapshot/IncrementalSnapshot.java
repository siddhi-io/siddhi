/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util.snapshot;

import java.util.Map;

/**
 * The class which represents the serialized incremental snapshot.
 */
public class IncrementalSnapshot {
    private Map<String, Map<String, byte[]>> incrementalState;
    private Map<String, Map<String, byte[]>> incrementalStateBase;
    private Map<String, Map<String, byte[]>> periodicState;


    public Map<String, Map<String, byte[]>> getIncrementalState() {
        return incrementalState;
    }

    public void setIncrementalState(Map<String, Map<String, byte[]>> incrementalState) {
        this.incrementalState = incrementalState;
    }

    public Map<String, Map<String, byte[]>> getIncrementalStateBase() {
        return incrementalStateBase;
    }

    public void setIncrementalStateBase(Map<String, Map<String, byte[]>> incrementalStateBase) {
        this.incrementalStateBase = incrementalStateBase;
    }

    public Map<String, Map<String, byte[]>> getPeriodicState() {
        return periodicState;
    }

    public void setPeriodicState(Map<String, Map<String, byte[]>> periodicState) {
        this.periodicState = periodicState;
    }

    @Override
    public String toString() {
        return "IncrementalSnapshot{" +
                "incrementalState=" + (incrementalState != null) +
                ", incrementalStateBase=" + (incrementalStateBase != null) +
                ", periodicState=" + (periodicState != null) +
                '}';
    }
}

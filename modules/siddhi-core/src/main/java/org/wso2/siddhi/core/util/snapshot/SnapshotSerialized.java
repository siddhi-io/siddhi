/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.util.snapshot;

import java.util.HashMap;

/**
 * The class which represents the serialized snapshot.
 */
public class SnapshotSerialized {
    private byte[] fullState;
    private HashMap<String, HashMap<String, Object>> incrementalState;
    private HashMap<String, HashMap<String, Object>> incrementalStateBase;

    public byte[] getFullState() {
        return fullState;
    }

    public void setFullState(byte[] fullState) {
        this.fullState = fullState;
    }

    public HashMap<String, HashMap<String, Object>> getIncrementalState() {
        return incrementalState;
    }

    public void setIncrementalState(HashMap<String, HashMap<String, Object>> incrementalState) {
        this.incrementalState = incrementalState;
    }

    public HashMap<String, HashMap<String, Object>> getIncrementalStateBase() {
        return incrementalStateBase;
    }

    public void setIncrementalStateBase(HashMap<String, HashMap<String, Object>> incrementalStateBase) {
        this.incrementalStateBase = incrementalStateBase;
    }
}

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

package io.siddhi.core.util.snapshot.state;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * The class which encloses multiple snapshot states.
 */
public class SnapshotStateList implements Serializable {
    private TreeMap<Long, Snapshot> snapshotStates = new TreeMap<>();

    public SnapshotStateList() {
    }

    public SnapshotStateList(TreeMap<Long, Snapshot> snapshotStates) {
        this.snapshotStates = snapshotStates;
    }

    public TreeMap<Long, Snapshot> getSnapshotStates() {
        return snapshotStates;
    }

    public void setSnapshotStates(TreeMap<Long, Snapshot> snapshotStates) {
        this.snapshotStates = snapshotStates;
    }

    public void putSnapshotState(Long id, Snapshot snapshot) {
        this.snapshotStates.put(id, snapshot);
    }
}

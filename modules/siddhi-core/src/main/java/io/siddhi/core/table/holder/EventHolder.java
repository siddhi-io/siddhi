/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.table.holder;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.util.snapshot.state.Snapshot;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;

/**
 * Base EventHolder interface. EventHolder is a container of {@link StreamEvent}s. You can add {@link ComplexEventChunk}
 * to the EventHolder. There are multiple event holders to fulfill different requirements.
 */
public interface EventHolder {
    void add(ComplexEventChunk<StreamEvent> addingEventChunk);

    Snapshot getSnapshot();

    void restore(SnapshotStateList snapshotStatelist);

    int size();

    void deleteAll();
}

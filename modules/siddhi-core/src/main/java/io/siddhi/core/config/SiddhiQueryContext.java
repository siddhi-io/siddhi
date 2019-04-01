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

package io.siddhi.core.config;

import io.siddhi.core.util.IdGenerator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.util.snapshot.state.EmptyStateHolder;
import io.siddhi.core.util.snapshot.state.PartitionStateHolder;
import io.siddhi.core.util.snapshot.state.PartitionSyncStateHolder;
import io.siddhi.core.util.snapshot.state.SingleStateHolder;
import io.siddhi.core.util.snapshot.state.SingleSyncStateHolder;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;

import java.util.Map;

/**
 * Holder object for context information of {@link SiddhiApp}.
 */
public class SiddhiQueryContext {

    private SiddhiAppContext siddhiAppContext = null;
    private String name;
    private String partitionId;
    private boolean partitioned;
    private OutputStream.OutputEventType outputEventType;
    private LatencyTracker latencyTracker;
    private Map<String, StateHolder> stateHolderMap;
    private IdGenerator idGenerator;

    public SiddhiQueryContext(SiddhiAppContext siddhiAppContext, String queryName) {
        this(siddhiAppContext, queryName, SiddhiConstants.PARTITION_ID_DEFAULT);
    }

    public SiddhiQueryContext(SiddhiAppContext siddhiAppContext, String queryName, String partitionId) {
        this.siddhiAppContext = siddhiAppContext;
        this.name = queryName;
        this.partitionId = partitionId;
        this.idGenerator = new IdGenerator();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SiddhiAppContext getSiddhiAppContext() {
        return siddhiAppContext;
    }

    public SiddhiContext getSiddhiContext() {
        return siddhiAppContext.getSiddhiContext();
    }

    public void setSiddhiAppContext(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
    }

    public void setOutputEventType(OutputStream.OutputEventType outputEventType) {
        this.outputEventType = outputEventType;
    }

    public OutputStream.OutputEventType getOutputEventType() {
        return outputEventType;
    }

    public void setLatencyTracker(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
    }

    public LatencyTracker getLatencyTracker() {
        return latencyTracker;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public void setPartitioned(boolean partitionable) {
        partitioned = partitionable;
    }

    public String generateNewId() {
        return idGenerator.createNewId();
    }

    public StateHolder generateStateHolder(String name, boolean groupBy, StateFactory stateFactory) {
        return generateStateHolder(name, groupBy, stateFactory, false);
    }

    public StateHolder generateStateHolder(String name, boolean groupBy, StateFactory stateFactory, boolean unSafe) {
        if (stateFactory != null) {
            StateHolder stateHolder;
            if (unSafe) {
                if (partitioned || groupBy) {
                    stateHolder = new PartitionStateHolder(stateFactory);
                } else {
                    stateHolder = new SingleStateHolder(stateFactory);
                }
            } else {
                if (partitioned || groupBy) {
                    stateHolder = new PartitionSyncStateHolder(stateFactory);
                } else {
                    stateHolder = new SingleSyncStateHolder(stateFactory);
                }
            }

            if (SnapshotService.getSkipStateStorageThreadLocal().get() == null ||
                    !SnapshotService.getSkipStateStorageThreadLocal().get()) {
                stateHolderMap = siddhiAppContext.getSnapshotService().getStateHolderMap(partitionId, this.getName());
                stateHolderMap.put(idGenerator.createNewId() + "-" + name, stateHolder);
            }
            return stateHolder;
        } else {
            return new EmptyStateHolder();
        }
    }
}

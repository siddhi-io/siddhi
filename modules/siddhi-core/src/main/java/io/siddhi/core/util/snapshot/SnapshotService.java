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
package io.siddhi.core.util.snapshot;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.CannotClearSiddhiAppStateException;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.NoPersistenceStoreException;
import io.siddhi.core.exception.PersistenceStoreException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.util.ThreadBarrier;
import io.siddhi.core.util.persistence.IncrementalPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.core.util.persistence.util.IncrementalSnapshotInfo;
import io.siddhi.core.util.persistence.util.PersistenceConstants;
import io.siddhi.core.util.persistence.util.PersistenceHelper;
import io.siddhi.core.util.snapshot.state.Snapshot;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service level implementation to take/restore snapshots of processing elements.
 * Memory   : PartitionId + QueryName + ElementId + PartitionGroupByKey + Item
 * Snapshot : PartitionId + PartitionGroupByKey + QueryName + ElementId + Item
 */
public class SnapshotService {
    private static final Logger log = LogManager.getLogger(SnapshotService.class);
    private static final ThreadLocal<Boolean> skipStateStorageThreadLocal = new ThreadLocal<Boolean>();
    private final ThreadBarrier threadBarrier;
    private ConcurrentHashMap<String, PartitionIdStateHolder> partitionIdStates;
    private SiddhiAppContext siddhiAppContext;

    public SnapshotService(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.threadBarrier = siddhiAppContext.getThreadBarrier();
        this.partitionIdStates = new ConcurrentHashMap<>();
    }

    public static ThreadLocal<Boolean> getSkipStateStorageThreadLocal() {
        return skipStateStorageThreadLocal;
    }

    public ConcurrentHashMap<String, PartitionIdStateHolder> getStates() {
        return partitionIdStates;
    }

    public Map<String, StateHolder> getStateHolderMap(String partitionId, String queryName) {
        Boolean skipSnapshotable = skipStateStorageThreadLocal.get();
        if (skipSnapshotable == null || !skipSnapshotable) {
            PartitionIdStateHolder partitionIdStateHolder = this.partitionIdStates.get(partitionId);
            if (partitionIdStateHolder == null) {
                partitionIdStateHolder = new PartitionIdStateHolder(partitionId);
                this.partitionIdStates.put(partitionId, partitionIdStateHolder);
            }
            ElementStateHolder elementStateHolder = partitionIdStateHolder.getElementState(queryName);
            if (elementStateHolder == null) {
                elementStateHolder = new ElementStateHolder(queryName, new HashMap<>());
                partitionIdStateHolder.addElementState(queryName, elementStateHolder);
            }
            return elementStateHolder.elementHolderMap;
        }
        return null;
    }

    public byte[] fullSnapshot() {
        try {
            SnapshotRequest.requestForFullSnapshot(true);
            Map<String, Map<String, Map<String, Map<String, Map<String, Object>>>>> fullSnapshot = new HashMap<>();
            byte[] serializedFullState = null;
            if (log.isDebugEnabled()) {
                log.debug("Taking snapshot ...");
            }
            try {
                threadBarrier.lock();
                waitForSystemStabilization();
                for (Map.Entry<String, PartitionIdStateHolder> partitionIdState : partitionIdStates.entrySet()) {
                    for (Map.Entry<String, ElementStateHolder> queryState :
                            partitionIdState.getValue().queryStateHolderMap.entrySet()) {
                        for (Map.Entry<String, StateHolder> elementState :
                                queryState.getValue().elementHolderMap.entrySet()) {
                            Map<String, Map<String, State>> partitionKeyStates = elementState.getValue().getAllStates();
                            try {
                                for (Map.Entry<String, Map<String, State>> partitionKeyState :
                                        partitionKeyStates.entrySet()) {
                                    for (Map.Entry<String, State> groupByKeyState :
                                            partitionKeyState.getValue().entrySet()) {
                                        String partitionAndGroupByKey = partitionKeyState.getKey() + "--" +
                                                groupByKeyState.getKey();
                                        State state = groupByKeyState.getValue();
                                        Map<String, Object> itemStates = state.snapshot();
                                        if (itemStates != null) {
                                            Map<String, Object> itemSnapshots = new HashMap<>();
                                            for (Map.Entry<String, Object> itemState : itemStates.entrySet()) {
                                                if (itemState.getValue() instanceof Snapshot) {
                                                    if (((Snapshot) itemState.getValue()).isIncrementalSnapshot()) {
                                                        throw new NoPersistenceStoreException("No incremental " +
                                                                "persistence store exist to store incremental " +
                                                                "snapshot of siddhiApp:'"
                                                                + siddhiAppContext.getName()
                                                                + "' subElement:'" + queryState.getKey()
                                                                + "' elementId:'" + elementState.getKey()
                                                                + "' partitionKey:'" + partitionKeyState.getKey()
                                                                + "' groupByKey:'" + groupByKeyState.getKey()
                                                                + "' and itemKey:'" + itemState.getKey() + "'");
                                                    } else {
                                                        itemSnapshots.put(itemState.getKey(), itemState.getValue());
                                                    }
                                                } else {
                                                    itemSnapshots.put(itemState.getKey(), itemState.getValue());
                                                }
                                            }
                                            Map<String, Map<String, Map<String, Map<String, Object>>>>
                                                    partitionIdSnapshot = fullSnapshot.computeIfAbsent(
                                                    partitionIdState.getKey(),
                                                    k -> new HashMap<>());
                                            Map<String, Map<String, Map<String, Object>>> partitionGroupByKeySnapshot =
                                                    partitionIdSnapshot.computeIfAbsent(
                                                            partitionAndGroupByKey,
                                                            k -> new HashMap<>());
                                            Map<String, Map<String, Object>> querySnapshot =
                                                    partitionGroupByKeySnapshot.computeIfAbsent(
                                                            queryState.getKey(),
                                                            k -> new HashMap<>());
                                            Map<String, Object> elementSnapshot =
                                                    querySnapshot.get(elementState.getKey());
                                            if (elementSnapshot == null) {
                                                querySnapshot.put(elementState.getKey(), itemSnapshots);
                                            } else {
                                                throw new SiddhiAppRuntimeException("Duplicate state exist for " +
                                                        "siddhiApp:'" + siddhiAppContext.getName()
                                                        + "' partitionKey:'" + partitionKeyState.getKey()
                                                        + "' groupByKey:'" + groupByKeyState.getKey()
                                                        + "' subElement:'" + queryState.getKey()
                                                        + "' elementId:'" + elementState.getKey() + "'");
                                            }
                                        }
                                    }
                                }
                            } finally {
                                elementState.getValue().returnAllStates(partitionKeyStates);
                            }
                        }
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("Snapshot serialization started ...");
                }
                serializedFullState = ByteSerializer.objectToByte(fullSnapshot, siddhiAppContext);
                if (log.isDebugEnabled()) {
                    log.debug("Snapshot serialization finished.");
                }
            } finally {
                threadBarrier.unlock();
            }
            if (log.isDebugEnabled()) {
                log.debug("Snapshot taken for Siddhi app '" + siddhiAppContext.getName() + "'");
            }
            return serializedFullState;
        } finally {
            SnapshotRequest.requestForFullSnapshot(false);
        }
    }

    public IncrementalSnapshot incrementalSnapshot() {
        try {
            SnapshotRequest.requestForFullSnapshot(false);
            Map<String, Map<String, byte[]>> incrementalSnapshotMap = new HashMap<>();
            Map<String, Map<String, byte[]>> incrementalBaseSnapshotMap = new HashMap<>();
            Map<String, Map<String, byte[]>> periodicSnapshotMap = new HashMap<>();
            if (log.isDebugEnabled()) {
                log.debug("Taking snapshot ...");
            }
            try {
                threadBarrier.lock();
                waitForSystemStabilization();
                for (Map.Entry<String, PartitionIdStateHolder> partitionIdState : partitionIdStates.entrySet()) {
                    for (Map.Entry<String, ElementStateHolder> queryState :
                            partitionIdState.getValue().queryStateHolderMap.entrySet()) {
                        for (Map.Entry<String, StateHolder> elementState :
                                queryState.getValue().elementHolderMap.entrySet()) {
                            Map<String, Map<String, State>> partitionKeyStates = elementState.getValue().getAllStates();
                            try {
                                for (Map.Entry<String, Map<String, State>> partitionKeyState :
                                        partitionKeyStates.entrySet()) {
                                    for (Map.Entry<String, State> groupByKeyState :
                                            partitionKeyState.getValue().entrySet()) {
                                        State state = groupByKeyState.getValue();
                                        Map<String, Object> itemStates = state.snapshot();
                                        if (itemStates != null) {
                                            Map<String, Object> itemSnapshotsIncremental = new HashMap<>();
                                            Map<String, Object> itemSnapshotsIncrementalBase = new HashMap<>();
                                            Map<String, Object> itemSnapshotsPeriodic = new HashMap<>();
                                            for (Map.Entry<String, Object> itemState : itemStates.entrySet()) {
                                                if (itemState.getValue() instanceof Snapshot) {
                                                    if (((Snapshot) itemState.getValue()).isIncrementalSnapshot()) {
                                                        itemSnapshotsIncremental.put(itemState.getKey(),
                                                                itemState.getValue());
                                                    } else {
                                                        itemSnapshotsIncrementalBase.put(
                                                                itemState.getKey(), itemState.getValue());
                                                    }
                                                } else {
                                                    itemSnapshotsPeriodic.put(itemState.getKey(), itemState.getValue());
                                                }
                                            }
                                            if (!itemSnapshotsIncremental.isEmpty()) {
                                                addToSnapshotIncrements(incrementalSnapshotMap, partitionIdState,
                                                        queryState, elementState, partitionKeyState, groupByKeyState,
                                                        itemSnapshotsIncremental);
                                            }
                                            if (!itemSnapshotsIncrementalBase.isEmpty()) {
                                                addToSnapshotIncrements(incrementalBaseSnapshotMap, partitionIdState,
                                                        queryState, elementState, partitionKeyState, groupByKeyState,
                                                        itemSnapshotsIncrementalBase);
                                            }
                                            if (!itemSnapshotsPeriodic.isEmpty()) {
                                                addToSnapshotIncrements(periodicSnapshotMap, partitionIdState,
                                                        queryState, elementState, partitionKeyState, groupByKeyState,
                                                        itemSnapshotsPeriodic);
                                            }
                                        }
                                    }
                                }
                            } finally {
                                elementState.getValue().returnAllStates(partitionKeyStates);
                            }
                        }
                    }
                }

            } finally {
                threadBarrier.unlock();
            }
            if (log.isDebugEnabled()) {
                log.debug("Snapshot taken for Siddhi app '" + siddhiAppContext.getName() + "'");
            }
            IncrementalSnapshot snapshot = new IncrementalSnapshot();
            if (!incrementalSnapshotMap.isEmpty()) {
                snapshot.setIncrementalState(incrementalSnapshotMap);
            }
            if (!incrementalBaseSnapshotMap.isEmpty()) {
                snapshot.setIncrementalStateBase(incrementalBaseSnapshotMap);
            }
            if (!periodicSnapshotMap.isEmpty()) {
                snapshot.setPeriodicState(periodicSnapshotMap);
            }
            return snapshot;
        } finally {
            SnapshotRequest.requestForFullSnapshot(false);
        }
    }

    private void addToSnapshotIncrements(Map<String, Map<String, byte[]>> incrementalSnapshotMap,
                                         Map.Entry<String, PartitionIdStateHolder> partitionIdState,
                                         Map.Entry<String, ElementStateHolder> queryState,
                                         Map.Entry<String, StateHolder> elementState,
                                         Map.Entry<String, Map<String, State>> partitionKeyState,
                                         Map.Entry<String, State> groupByKeyState,
                                         Map<String, Object> itemSnapshotsIncremental) {
        String id = partitionKeyState.getKey() + "--" + groupByKeyState.getKey() +
                PersistenceConstants.REVISION_SEPARATOR + queryState.getKey() +
                PersistenceConstants.REVISION_SEPARATOR + elementState.getKey();
        Map<String, byte[]> partitionIdSnapshot =
                incrementalSnapshotMap.computeIfAbsent(
                        partitionIdState.getKey(),
                        k -> new HashMap<>());
        partitionIdSnapshot.put(id, ByteSerializer.objectToByte(
                itemSnapshotsIncremental, siddhiAppContext));
    }

    public Map<String, Object> queryState(String queryName) {
        Map<String, Object> queryState = new HashMap<>();
        try {
            // Lock the threads in Siddhi
            threadBarrier.lock();
            waitForSystemStabilization();
            PartitionIdStateHolder partitionIdStateHolder = partitionIdStates.get("");
            if (partitionIdStateHolder != null) {
                ElementStateHolder elementStateHolder = partitionIdStateHolder.queryStateHolderMap.get(queryName);
                if (elementStateHolder != null) {
                    for (Map.Entry<String, StateHolder> elementState : elementStateHolder.elementHolderMap.entrySet()) {
                        Map<String, Map<String, State>> partitionKeyStates = elementState.getValue().getAllStates();
                        try {
                            for (Map.Entry<String, Map<String, State>> partitionKeyState :
                                    partitionKeyStates.entrySet()) {
                                for (Map.Entry<String, State> groupByKeyState :
                                        partitionKeyState.getValue().entrySet()) {
                                    String id = partitionKeyState.getKey() + "--" + groupByKeyState.getKey() + "_"
                                            + queryName + "_" + elementState.getKey();
                                    queryState.put(id, groupByKeyState.getValue().snapshot());
                                }
                            }
                        } finally {
                            elementState.getValue().returnAllStates(partitionKeyStates);
                        }
                    }
                }
            }
        } finally {
            threadBarrier.unlock();
        }
        if (log.isDebugEnabled()) {
            log.debug("Taking snapshot finished.");
        }
        return queryState;
    }

    public void restore(byte[] snapshot) throws CannotRestoreSiddhiAppStateException {
        if (snapshot == null) {
            throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                    getName() + " failed due to no snapshot.");
        }
        Map<String, Map<String, Map<String, Map<String, Map<String, Object>>>>> fullSnapshot =
                (Map<String, Map<String, Map<String, Map<String, Map<String, Object>>>>>)
                        ByteSerializer.byteToObject(snapshot, siddhiAppContext);
        if (fullSnapshot == null) {
            throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                    getName() + " failed due to invalid snapshot.");
        }
        try {
            threadBarrier.lock();
            waitForSystemStabilization();
            try {
                //cleaning old group by states
                cleanGroupByStates();
                //restore data
                for (Map.Entry<String, Map<String, Map<String, Map<String, Map<String, Object>>>>> partitionIdSnapshot :
                        fullSnapshot.entrySet()) {
                    PartitionIdStateHolder partitionStateHolder = partitionIdStates.get(partitionIdSnapshot.getKey());
                    if (partitionStateHolder == null) {
                        continue;
                    }
                    for (Map.Entry<String, Map<String, Map<String, Map<String, Object>>>> partitionGroupByKeySnapshot :
                            partitionIdSnapshot.getValue().entrySet()) {
                        for (Map.Entry<String, Map<String, Map<String, Object>>> querySnapshot :
                                partitionGroupByKeySnapshot.getValue().entrySet()) {
                            ElementStateHolder elementStateHolder =
                                    partitionStateHolder.queryStateHolderMap.get(querySnapshot.getKey());
                            if (elementStateHolder == null) {
                                continue;
                            }
                            for (Map.Entry<String, Map<String, Object>> elementSnapshot :
                                    querySnapshot.getValue().entrySet()) {
                                StateHolder stateHolder =
                                        elementStateHolder.elementHolderMap.get(elementSnapshot.getKey());
                                if (stateHolder == null) {
                                    continue;
                                }
                                try {
                                    String partitionKey = null;
                                    String groupByKey = null;
                                    if (partitionGroupByKeySnapshot.getKey() != null) {
                                        String[] keys = partitionGroupByKeySnapshot.getKey().split("--");
                                        if (keys.length == 2) {
                                            if (!keys[0].equals("null")) {
                                                partitionKey = keys[0];
                                            }
                                            if (!keys[1].equals("null")) {
                                                groupByKey = keys[1];
                                            }
                                        }
                                    }
                                    SiddhiAppContext.startPartitionFlow(partitionKey);
                                    SiddhiAppContext.startGroupByFlow(groupByKey);
                                    State state = stateHolder.getState();
                                    try {
                                        if (state == null) {
                                            continue;
                                        }
                                        Map<String, Object> snapshotRestores = new HashMap<>();
                                        for (Map.Entry<String, Object> itemSnapshot :
                                                elementSnapshot.getValue().entrySet()) {
                                            if (itemSnapshot.getValue() instanceof Snapshot) {
                                                SnapshotStateList snapshotStateList = new SnapshotStateList();
                                                snapshotStateList.putSnapshotState(0L,
                                                        (Snapshot) itemSnapshot.getValue());
                                                snapshotRestores.put(itemSnapshot.getKey(), snapshotStateList);
                                            } else {
                                                snapshotRestores.put(itemSnapshot.getKey(),
                                                        itemSnapshot.getValue());
                                            }
                                        }
                                        state.restore(snapshotRestores);
                                    } finally {
                                        stateHolder.returnState(state);
                                    }
                                } finally {
                                    SiddhiAppContext.stopPartitionFlow();
                                    SiddhiAppContext.stopGroupByFlow();
                                }
                            }
                        }
                    }

                }
            } catch (Throwable t) {
                throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " +
                        siddhiAppContext.getName() + " not completed properly because content of Siddhi " +
                        "app has changed since last state persistence. Clean persistence store for a " +
                        "fresh deployment.", t);
            }
        } finally {
            threadBarrier.unlock();
        }
    }

    public void restore(Map<String, Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>>
                                snapshot)
            throws CannotRestoreSiddhiAppStateException {
        try {
            threadBarrier.lock();
            waitForSystemStabilization();
            try {
                //cleaning old group by states
                cleanGroupByStates();
                //restore data
                for (Map.Entry<String, Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>>
                        partitionIdSnapshot : snapshot.entrySet()) {
                    PartitionIdStateHolder partitionStateHolder = partitionIdStates.get(partitionIdSnapshot.getKey());
                    if (partitionStateHolder == null) {
                        continue;
                    }
                    for (Iterator<Map.Entry<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>>
                         iterator = partitionIdSnapshot.getValue().entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>
                                partitionGroupByKeySnapshot = iterator.next();
                        restoreIncrementalSnapshot(partitionStateHolder, partitionGroupByKeySnapshot.getValue());
                        iterator.remove();
                    }

                }
            } catch (Throwable t) {
                throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " +
                        siddhiAppContext.getName() + " not completed properly because content of Siddhi " +
                        "app has changed since last state persistence. Clean persistence store for a " +
                        "fresh deployment.", t);
            }
        } finally {
            threadBarrier.unlock();
        }
    }

    private void restoreIncrementalSnapshot(PartitionIdStateHolder partitionIdStateHolder,
                                            Map<String, Map<Long, Map<IncrementalSnapshotInfo,
                                                    byte[]>>> incrementalStateByTime) {
        if (incrementalStateByTime != null) {
            String id = null;
            State state = null;
            StateHolder stateHolder = null;
            Map<String, Object> deserializedStateMap = null;
            try {
                for (Iterator<Map.Entry<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>> iterator =
                     incrementalStateByTime.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>> incrementalStateByTimeEntry =
                            iterator.next();
                    iterator.remove();
                    for (Iterator<Map.Entry<Long, Map<IncrementalSnapshotInfo, byte[]>>> partitionGroupByKeyIterator =
                         incrementalStateByTimeEntry.getValue().entrySet().iterator();
                         partitionGroupByKeyIterator.hasNext(); ) {
                        Map.Entry<Long, Map<IncrementalSnapshotInfo, byte[]>>
                                partitionGroupByKeyStateByTimeEntry = partitionGroupByKeyIterator.next();
                        partitionGroupByKeyIterator.remove();
                        for (Iterator<Map.Entry<IncrementalSnapshotInfo, byte[]>> iterator1 =
                             partitionGroupByKeyStateByTimeEntry.getValue().entrySet().iterator();
                             iterator1.hasNext(); ) {
                            Map.Entry<IncrementalSnapshotInfo, byte[]> incrementalStateByInfoEntry = iterator1.next();
                            iterator1.remove();
                            IncrementalSnapshotInfo incrementalSnapshotInfo = incrementalStateByInfoEntry.getKey();
                            Map<String, Object> singleIncrementSnapshot = (Map<String, Object>)
                                    ByteSerializer.byteToObject(
                                            incrementalStateByInfoEntry.getValue(),
                                            siddhiAppContext);
                            if (singleIncrementSnapshot != null) {
                                if (!incrementalSnapshotInfo.getId().equals(id)) {
                                    if (id != null) {
                                        state.restore(deserializedStateMap);
                                        SiddhiAppContext.startPartitionFlow(id);
                                        try {
                                            stateHolder.returnState(state);
                                        } finally {
                                            SiddhiAppContext.stopPartitionFlow();
                                        }
                                        id = null;
                                        state = null;
                                        stateHolder = null;
                                        deserializedStateMap = null;
                                    }
                                    ElementStateHolder elementStateHolder = partitionIdStateHolder.
                                            queryStateHolderMap.get(incrementalSnapshotInfo.getQueryName());
                                    if (elementStateHolder == null) {
                                        continue;
                                    }
                                    stateHolder = elementStateHolder.elementHolderMap.get(
                                            incrementalSnapshotInfo.getElementId());
                                    if (stateHolder == null) {
                                        continue;
                                    }
                                    String partitionKey = null;
                                    String groupByKey = null;
                                    String[] keys = incrementalSnapshotInfo.getPartitionGroupByKey().split("--");
                                    if (keys.length == 2) {
                                        if (!keys[0].equals("null")) {
                                            partitionKey = keys[0];
                                        }
                                        if (!keys[1].equals("null")) {
                                            groupByKey = keys[1];
                                        }
                                    }
                                    SiddhiAppContext.startPartitionFlow(partitionKey);
                                    SiddhiAppContext.startGroupByFlow(groupByKey);
                                    try {
                                        state = stateHolder.getState();
                                    } finally {
                                        SiddhiAppContext.stopGroupByFlow();
                                        SiddhiAppContext.stopPartitionFlow();
                                    }
                                    if (state != null) {
                                        id = incrementalSnapshotInfo.getId();
                                        deserializedStateMap = new HashMap<>();
                                    }
                                }
                                if (state != null) {
                                    for (Map.Entry<String, Object> singleIncrementSnapshotEntry :
                                            singleIncrementSnapshot.entrySet()) {
                                        if (singleIncrementSnapshotEntry.getValue() instanceof Snapshot) {
                                            Snapshot snapshot = (Snapshot)
                                                    singleIncrementSnapshotEntry.getValue();
                                            SnapshotStateList snapshotStateList = (SnapshotStateList)
                                                    deserializedStateMap.computeIfAbsent(
                                                            singleIncrementSnapshotEntry.getKey(),
                                                            k -> new SnapshotStateList());
                                            if (snapshot.isIncrementalSnapshot()) {
                                                snapshotStateList.putSnapshotState(
                                                        partitionGroupByKeyStateByTimeEntry.getKey(),
                                                        snapshot);
                                            } else {
                                                snapshotStateList.getSnapshotStates().clear();
                                                snapshotStateList.putSnapshotState(
                                                        partitionGroupByKeyStateByTimeEntry.getKey(),
                                                        snapshot);
                                            }
                                        } else {
                                            deserializedStateMap.put(singleIncrementSnapshotEntry.getKey(),
                                                    singleIncrementSnapshotEntry.getValue());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (id != null) {
                        state.restore(deserializedStateMap);
                        SiddhiAppContext.startPartitionFlow(id);
                        try {
                            stateHolder.returnState(state);
                        } finally {
                            SiddhiAppContext.stopPartitionFlow();
                        }
                        id = null;
                        state = null;
                        stateHolder = null;
                        deserializedStateMap = null;
                    }
                }
            } finally {
                if (id != null && stateHolder != null && state != null) {
                    SiddhiAppContext.startPartitionFlow(id);
                    try {
                        stateHolder.returnState(state);
                    } finally {
                        SiddhiAppContext.stopPartitionFlow();
                    }
                    id = null;
                    state = null;
                    stateHolder = null;
                }
            }

        }
    }

    public void restoreRevision(String revision) throws CannotRestoreSiddhiAppStateException {
        PersistenceStore persistenceStore = siddhiAppContext.getSiddhiContext().getPersistenceStore();
        IncrementalPersistenceStore incrementalPersistenceStore =
                siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore();
        String siddhiAppName = siddhiAppContext.getName();
        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Restoring revision: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(siddhiAppContext.getName(), revision);
            if (snapshot != null) {
                restore(snapshot);
                if (log.isDebugEnabled()) {
                    log.debug("Restored revision: " + revision);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No data found for revision: " + revision);
                }
                throw new PersistenceStoreException("No data found for revision: " + revision);
            }
        } else if (incrementalPersistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Restoring revision: " + revision + " ...");
            }
            IncrementalSnapshotInfo restoreSnapshotInfo = PersistenceHelper.convertRevision(revision);
            List<IncrementalSnapshotInfo> incrementalSnapshotInfos =
                    incrementalPersistenceStore.getListOfRevisionsToLoad(
                            restoreSnapshotInfo.getTime(), restoreSnapshotInfo.getSiddhiAppId());
            if (incrementalSnapshotInfos != null) {
                incrementalSnapshotInfos.sort(new Comparator<IncrementalSnapshotInfo>() {
                    @Override
                    public int compare(IncrementalSnapshotInfo o1, IncrementalSnapshotInfo o2) {
                        int results = o1.getId().compareTo(o2.getId());
                        if (results == 0) {
                            results = Long.compare(o2.getTime(), o1.getTime());
                            if (results == 0) {
                                return o2.getType().compareTo(o1.getType());
                            }
                        }
                        return results;
                    }
                });
                String lastId = null;
                boolean baseFound = false;
                boolean perioicFound = false;
                for (Iterator<IncrementalSnapshotInfo> iterator = incrementalSnapshotInfos.iterator();
                     iterator.hasNext(); ) {
                    IncrementalSnapshotInfo snapshotInfo = iterator.next();
                    if (snapshotInfo.getId().equals(lastId)) {
                        if (baseFound && (snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.BASE
                                || snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.INCREMENT)) {
                            iterator.remove();
                        } else if (perioicFound &&
                                snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.PERIODIC) {
                            iterator.remove();
                        } else if (snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.BASE) {
                            baseFound = true;
                        } else if (snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.PERIODIC) {
                            perioicFound = true;
                        }
                    } else {
                        baseFound = snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.BASE;
                        perioicFound = snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.PERIODIC;
                    }
                    lastId = snapshotInfo.getId();
                }
                Map<String, Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>>
                        incrementalState = new HashMap<>();
                for (IncrementalSnapshotInfo snapshotInfo : incrementalSnapshotInfos) {
                    Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>>
                            incrementalStateByPartitionGroupByKey = incrementalState.computeIfAbsent(
                            snapshotInfo.getPartitionId(), k -> new TreeMap<>());
                    Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>> incrementalStateByTime =
                            incrementalStateByPartitionGroupByKey.computeIfAbsent(snapshotInfo.getPartitionGroupByKey(),
                                    k -> new TreeMap<>());
                    Map<Long, Map<IncrementalSnapshotInfo, byte[]>> idByTime =
                            incrementalStateByTime.computeIfAbsent(snapshotInfo.getId(),
                                    k -> new TreeMap<>());
                    Map<IncrementalSnapshotInfo, byte[]> incrementalStateByInfo = idByTime.
                            computeIfAbsent(snapshotInfo.getTime(), k -> new HashMap<>());
                    incrementalStateByInfo.put(snapshotInfo, incrementalPersistenceStore.load(snapshotInfo));
                }
                restore(incrementalState);
                if (log.isDebugEnabled()) {
                    log.debug("Restored revision: " + revision);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No data found for revision: " + revision);
                }
                throw new PersistenceStoreException("No data found for revision: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }
    }

    public String restoreLastRevision() throws CannotRestoreSiddhiAppStateException {
        PersistenceStore persistenceStore = siddhiAppContext.getSiddhiContext().getPersistenceStore();
        IncrementalPersistenceStore incrementalPersistenceStore =
                siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore();
        String siddhiAppName = siddhiAppContext.getName();
        String revision;
        if (persistenceStore != null) {
            revision = persistenceStore.getLastRevision(siddhiAppName);
        } else if (incrementalPersistenceStore != null) {
            revision = incrementalPersistenceStore.getLastRevision(siddhiAppName);
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " + siddhiAppName);
        }
        if (revision != null) {
            restoreRevision(revision);
        }
        return revision;
    }

    /**
     * Clear all the revisions of persistence store of Siddhi App
     */
    public void clearAllRevisions() throws CannotClearSiddhiAppStateException {
        PersistenceStore persistenceStore = siddhiAppContext.getSiddhiContext().getPersistenceStore();
        IncrementalPersistenceStore incrementalPersistenceStore =
                siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore();
        String siddhiAppName = siddhiAppContext.getName();
        if (persistenceStore != null) {
            persistenceStore.clearAllRevisions(siddhiAppName);
        } else if (incrementalPersistenceStore != null) {
            incrementalPersistenceStore.clearAllRevisions(siddhiAppName);
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " + siddhiAppName);
        }
    }

    private void cleanGroupByStates() {
        for (Map.Entry<String, PartitionIdStateHolder> partitionIdState : partitionIdStates.entrySet()) {
            for (Map.Entry<String, ElementStateHolder> queryState :
                    partitionIdState.getValue().queryStateHolderMap.entrySet()) {
                for (Map.Entry<String, StateHolder> elementState :
                        queryState.getValue().elementHolderMap.entrySet()) {
                    elementState.getValue().cleanGroupByStates();
                }
            }
        }
    }

    private void waitForSystemStabilization() {
        int retryCount = 100;
        int activeThreads = siddhiAppContext.getThreadBarrier().getActiveThreads();
        while (activeThreads != 0 && retryCount > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new SiddhiAppRuntimeException("Stabilization of Siddhi App " + siddhiAppContext.getName() +
                        " for snapshot/restore interrupted. " + e.getMessage(), e);
            }
            activeThreads = siddhiAppContext.getThreadBarrier().getActiveThreads();
            retryCount--;
        }
        if (retryCount == 0) {
            throw new SiddhiAppRuntimeException("Siddhi App " + siddhiAppContext.getName() +
                    " not stabilized for snapshot/restore, Active thread count is " +
                    activeThreads);
        }
    }

    class PartitionIdStateHolder {
        private final String partitionId;
        private final Map<String, ElementStateHolder> queryStateHolderMap = new HashMap<>();

        public PartitionIdStateHolder(String partitionId) {
            this.partitionId = partitionId;
        }

        public void addElementState(String queryName, ElementStateHolder elementStateHolder) {
            queryStateHolderMap.put(queryName, elementStateHolder);
        }

        public ElementStateHolder getElementState(String queryName) {
            return queryStateHolderMap.get(queryName);
        }
    }

    class ElementStateHolder {
        private final String elementId;
        private final Map<String, StateHolder> elementHolderMap;

        public ElementStateHolder(String elementId, Map<String, StateHolder> elementHolderMap) {
            this.elementId = elementId;
            this.elementHolderMap = elementHolderMap;
        }
    }
}

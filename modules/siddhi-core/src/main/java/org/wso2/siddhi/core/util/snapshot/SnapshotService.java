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
package org.wso2.siddhi.core.util.snapshot;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.exception.PersistenceStoreException;
import org.wso2.siddhi.core.util.ThreadBarrier;
import org.wso2.siddhi.core.util.persistence.IncrementalPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.core.util.persistence.util.IncrementalSnapshotInfo;
import org.wso2.siddhi.core.util.persistence.util.PersistenceHelper;
import org.wso2.siddhi.core.util.snapshot.state.SnapshotState;
import org.wso2.siddhi.core.util.snapshot.state.SnapshotStateList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service level implementation to take/restore snapshots of processing elements.
 */
public class SnapshotService {
    private static final Logger log = Logger.getLogger(SnapshotService.class);
    private static final ThreadLocal<Boolean> skipSnapshotableThreadLocal = new ThreadLocal<Boolean>();

    private final ThreadBarrier threadBarrier;
    private ConcurrentHashMap<String, List<Snapshotable>> snapshotableMap = new ConcurrentHashMap<>();
    private SiddhiAppContext siddhiAppContext;

    public SnapshotService(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.threadBarrier = siddhiAppContext.getThreadBarrier();
    }

    public static ThreadLocal<Boolean> getSkipSnapshotableThreadLocal() {
        return skipSnapshotableThreadLocal;
    }

    public ConcurrentHashMap<String, List<Snapshotable>> getSnapshotableMap() {
        return snapshotableMap;
    }

    public synchronized void addSnapshotable(String queryName, Snapshotable snapshotable) {
        Boolean skipSnapshotable = skipSnapshotableThreadLocal.get();
        if (skipSnapshotable == null || !skipSnapshotable) {
            List<Snapshotable> snapshotableList = snapshotableMap.get(queryName);

            // If List does not exist create it.
            if (snapshotableList == null) {
                snapshotableList = new ArrayList<Snapshotable>();
                snapshotableList.add(snapshotable);
                snapshotableMap.put(queryName, snapshotableList);
            } else {
                // add if item is not already in list
                if (!snapshotableList.contains(snapshotable)) {
                    snapshotableList.add(snapshotable);
                }
            }
        }
    }

    public byte[] fullSnapshot() {
        try {
            SnapshotRequest.requestForFullSnapshot(true);
            Map<String, Map<String, Object>> elementSnapshotMapFull = new HashMap<>();
            byte[] serializedFullState = null;
            if (log.isDebugEnabled()) {
                log.debug("Taking snapshot ...");
            }
            try {
                threadBarrier.lock();
                for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                    Map<String, Object> elementWiseFullSnapshots = new HashMap<>();
                    for (Snapshotable snapshotableObject : entry.getValue()) {
                        Map<String, Object> currentState = snapshotableObject.currentState();
                        if (currentState != null) {
                            Map<String, Object> elementWiseSnapshots = new HashMap<>();
                            for (Map.Entry<String, Object> item2 : currentState.entrySet()) {
                                String key = item2.getKey();
                                Object snapShot = item2.getValue();
                                if (snapShot instanceof SnapshotState) {
                                    if (((SnapshotState) snapShot).isIncrementalSnapshot()) {
                                        throw new NoPersistenceStoreException("No incremental persistence store " +
                                                "exist to store incremental snapshot of siddhiApp:'"
                                                + siddhiAppContext.getName() + "' subElement:'" + entry.getKey()
                                                + "' elementId:'" + snapshotableObject.getElementId()
                                                + "' and key:'" + key + "'");
                                    } else {
                                        elementWiseSnapshots.put(key, snapShot);
                                    }
                                } else {
                                    elementWiseSnapshots.put(key, snapShot);
                                }
                            }
                            if (!elementWiseSnapshots.isEmpty()) {
                                elementWiseFullSnapshots.put(snapshotableObject.getElementId(), elementWiseSnapshots);
                            }
                        }
                    }
                    if (!elementWiseFullSnapshots.isEmpty()) {
                        elementSnapshotMapFull.put(entry.getKey(), elementWiseFullSnapshots);
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("SnapshotState serialization started ...");
                }
                serializedFullState = ByteSerializer.objectToByte(elementSnapshotMapFull, siddhiAppContext);
                if (log.isDebugEnabled()) {
                    log.debug("SnapshotState serialization finished.");
                }
            } finally {
                threadBarrier.unlock();
            }
            if (log.isDebugEnabled()) {
                log.debug("SnapshotState taken for Siddhi app '" + siddhiAppContext.getName() + "'");
            }
            return serializedFullState;
        } finally {
            SnapshotRequest.requestForFullSnapshot(false);
        }
    }

    public IncrementalSnapshot incrementalSnapshot() {
        try {
            SnapshotRequest.requestForFullSnapshot(false);
            Map<String, Map<String, byte[]>> elementSnapshotMapIncremental = new HashMap<>();
            Map<String, Map<String, byte[]>> elementSnapshotMapIncrementalBase = new HashMap<>();
            Map<String, Map<String, byte[]>> elementSnapshotMapPeriodic = new HashMap<>();
            if (log.isDebugEnabled()) {
                log.debug("Taking snapshot ...");
            }
            try {
                threadBarrier.lock();
                for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                    Map<String, byte[]> elementWiseIncrementalSnapshots = new HashMap<>();
                    Map<String, byte[]> elementWiseIncrementalSnapshotsBase = new HashMap<>();
                    Map<String, byte[]> elementWisePeriodicSnapshots = new HashMap<>();
                    for (Snapshotable snapshotableObject : entry.getValue()) {
                        Map<String, Object> currentState = snapshotableObject.currentState();
                        if (currentState != null) {
                            Map<String, Object> incrementalSnapshotableMap = new HashMap<>();
                            Map<String, Object> incrementalSnapshotableMapBase = new HashMap<>();
                            Map<String, Object> periodicSnapshotableMap = new HashMap<>();
                            for (Map.Entry<String, Object> stateEntry : currentState.entrySet()) {
                                String key = stateEntry.getKey();
                                Object snapShot = stateEntry.getValue();
                                if (snapShot instanceof SnapshotState) {
                                    if (((SnapshotState) snapShot).isIncrementalSnapshot()) {
                                        incrementalSnapshotableMap.put(key, snapShot);
                                    } else {
                                        incrementalSnapshotableMapBase.put(key, snapShot);
                                    }
                                } else {
                                    periodicSnapshotableMap.put(key, snapShot);
                                }
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("SnapshotState serialization started ...");
                            }
                            if (!incrementalSnapshotableMap.isEmpty()) {
                                //Do we need to get and then update?
                                elementWiseIncrementalSnapshots.put(snapshotableObject.getElementId(),
                                        ByteSerializer.objectToByte(incrementalSnapshotableMap, siddhiAppContext));
                            }
                            if (!incrementalSnapshotableMapBase.isEmpty()) {
                                elementWiseIncrementalSnapshotsBase.put(snapshotableObject.getElementId(),
                                        ByteSerializer.objectToByte(incrementalSnapshotableMapBase, siddhiAppContext));
                            }
                            if (!periodicSnapshotableMap.isEmpty()) {
                                elementWisePeriodicSnapshots.put(snapshotableObject.getElementId(),
                                        ByteSerializer.objectToByte(periodicSnapshotableMap, siddhiAppContext));
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("SnapshotState serialization finished.");
                            }
                        }
                    }
                    if (!elementWiseIncrementalSnapshots.isEmpty()) {
                        elementSnapshotMapIncremental.put(entry.getKey(), elementWiseIncrementalSnapshots);
                    }
                    if (!elementWiseIncrementalSnapshotsBase.isEmpty()) {
                        elementSnapshotMapIncrementalBase.put(entry.getKey(), elementWiseIncrementalSnapshotsBase);
                    }
                    if (!elementWisePeriodicSnapshots.isEmpty()) {
                        elementSnapshotMapPeriodic.put(entry.getKey(), elementWisePeriodicSnapshots);
                    }
                }
            } finally {
                threadBarrier.unlock();
            }
            if (log.isDebugEnabled()) {
                log.debug("SnapshotState taken for Siddhi app '" + siddhiAppContext.getName() + "'");
            }
            IncrementalSnapshot snapshot = new IncrementalSnapshot();
            if (!elementSnapshotMapIncremental.isEmpty()) {
                snapshot.setIncrementalState(elementSnapshotMapIncremental);
            }
            if (!elementSnapshotMapIncrementalBase.isEmpty()) {
                snapshot.setIncrementalStateBase(elementSnapshotMapIncrementalBase);
            }
            if (!elementSnapshotMapPeriodic.isEmpty()) {
                snapshot.setPeriodicState(elementSnapshotMapPeriodic);
            }
            return snapshot;
        } finally {
            SnapshotRequest.requestForFullSnapshot(false);
        }
    }

    public Map<String, Object> queryState(String queryName) {
        Map<String, Object> state = new HashMap<>();
        try {
            // Lock the threads in Siddhi
            threadBarrier.lock();
            List<Snapshotable> list = snapshotableMap.get(queryName);

            if (list != null) {
                for (Snapshotable element : list) {
                    Map<String, Object> elementState = element.currentState();
                    String elementId = element.getElementId();
                    state.put(elementId, elementState);
                }
            }
        } finally {
            threadBarrier.unlock();
        }
        log.debug("Taking snapshot finished.");
        return state;
    }

    public void restore(byte[] snapshot) throws CannotRestoreSiddhiAppStateException {
        if (snapshot == null) {
            throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                    getName() + " failed due to no snapshot.");
        }
        Map<String, Map<String, Map<String, Object>>> snapshotsByQueryName =
                (Map<String, Map<String, Map<String, Object>>>) ByteSerializer.byteToObject(snapshot, siddhiAppContext);
        if (snapshotsByQueryName == null) {
            throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                    getName() + " failed due to invalid snapshot.");
        }
        try {
            threadBarrier.lock();
            if (snapshotableMap.containsKey("partition")) {
                List<Snapshotable> partitionSnapshotables = snapshotableMap.get("partition");

                try {
                    if (partitionSnapshotables != null) {
                        for (Snapshotable snapshotable : partitionSnapshotables) {
                            Map<String, Map<String, Object>> snapshotsByElementId =
                                    snapshotsByQueryName.get("partition");
                            snapshotable.restoreState(snapshotsByElementId.get(snapshotable.getElementId()));
                        }
                    }
                } catch (Throwable t) {
                    throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                            getName() + " not completed properly. This can occur if the content of Siddhi app has " +
                            "changed since it was last state persisted, or if the Siddhi app was not given a name. " +
                            "Make sure to provide a name to the Siddhi app by adding '@app:name('<a name>')' " +
                            "annotation and clean the persistence store if you have done modifications " +
                            "to the Siddhi app such that it can perform a fresh deployment.", t);
                }
            }

            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                if (!entry.getKey().equals("partition")) {
                    List<Snapshotable> snapshotableList = entry.getValue();
                    try {
                        for (Snapshotable snapshotable : snapshotableList) {
                            Map<String, Map<String, Object>> snapshotsByElementId =
                                    snapshotsByQueryName.get(entry.getKey());
                            if (snapshotsByElementId != null) {
                                Map<String, Object> snapshotsByKey = snapshotsByElementId.get(
                                        snapshotable.getElementId());
                                if (snapshotsByKey != null) {
                                    Map<String, Object> snapshotRestoresByKey = new HashMap<>();
                                    for (Map.Entry<String, Object> snapshotsByKeyEntry : snapshotsByKey.entrySet()) {
                                        if (snapshotsByKeyEntry.getValue() instanceof SnapshotState) {
                                            SnapshotStateList snapshotStateList = new SnapshotStateList();
                                            snapshotStateList.putSnapshotState(0L,
                                                    (SnapshotState) snapshotsByKeyEntry.getValue());
                                            snapshotRestoresByKey.put(snapshotsByKeyEntry.getKey(), snapshotStateList);
                                        } else {
                                            snapshotRestoresByKey.put(snapshotsByKeyEntry.getKey(),
                                                    snapshotsByKeyEntry.getValue());
                                        }
                                    }
                                    snapshotable.restoreState(snapshotRestoresByKey);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " +
                                siddhiAppContext.getName() + " not completed properly because content of Siddhi " +
                                "app has changed since last state persistence. Clean persistence store for a " +
                                "fresh deployment.", t);
                    }
                }
            }
        } finally {
            threadBarrier.unlock();
        }
    }

    public void restore(Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>> snapshot)
            throws CannotRestoreSiddhiAppStateException {
        try {
            threadBarrier.lock();
            if (snapshotableMap.containsKey("partition")) {
                List<Snapshotable> partitionSnapshotables = snapshotableMap.get("partition");

                try {
                    if (partitionSnapshotables != null) {
                        for (Snapshotable snapshotable : partitionSnapshotables) {
                            Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>> incrementalStateByElementId
                                    = snapshot.get("partition");
                            restoreIncrementalSnapshot(snapshotable, incrementalStateByElementId);
                        }
                    }
                } catch (Throwable t) {
                    throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                            getName() + " not completed properly because content of Siddhi app has changed since "
                            + "last state persistence. Clean persistence store for a fresh deployment.", t);
                }
            }

            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                if (!entry.getKey().equals("partition")) {
                    List<Snapshotable> snapshotableList = entry.getValue();
                    try {
                        for (Snapshotable snapshotable : snapshotableList) {
                            Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>> incrementalStateByElementId
                                    = snapshot.get(entry.getKey());
                            restoreIncrementalSnapshot(snapshotable, incrementalStateByElementId);
                        }
                    } catch (Throwable t) {
                        throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " +
                                siddhiAppContext.getName() + " not completed properly because content of Siddhi " +
                                "app has changed since last state persistence. Clean persistence store for a " +
                                "fresh deployment.", t);
                    }
                }
            }
        } finally {
            threadBarrier.unlock();
        }
    }

    private void restoreIncrementalSnapshot(Snapshotable snapshotable,
                                            Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>
                                                    incrementalStateByElementId) {
        if (incrementalStateByElementId != null) {
            Map<Long, Map<IncrementalSnapshotInfo, byte[]>> incrementalStateByTime
                    = incrementalStateByElementId.get(snapshotable.getElementId());
            if (incrementalStateByTime != null) {
                Map<String, Object> deserializedElementStateMap = new HashMap<>();
                for (Map.Entry<Long, Map<IncrementalSnapshotInfo, byte[]>> incrementalStateByTimeEntry :
                        incrementalStateByTime.entrySet()) {
                    for (Map.Entry<IncrementalSnapshotInfo, byte[]> incrementalStateByInfoEntry :
                            incrementalStateByTimeEntry.getValue().entrySet()) {
                        Map<String, Object> singleIncrementSnapshot = (Map<String, Object>)
                                ByteSerializer.byteToObject(incrementalStateByInfoEntry.getValue(), siddhiAppContext);
                        if (singleIncrementSnapshot != null) {
                            for (Map.Entry<String, Object> singleIncrementSnapshotEntry :
                                    singleIncrementSnapshot.entrySet()) {
                                if (singleIncrementSnapshotEntry.getValue() instanceof SnapshotState) {
                                    SnapshotState snapshotState = (SnapshotState)
                                            singleIncrementSnapshotEntry.getValue();
                                    SnapshotStateList snapshotStateList = (SnapshotStateList)
                                            deserializedElementStateMap.computeIfAbsent(
                                                    singleIncrementSnapshotEntry.getKey(),
                                                    k -> new SnapshotStateList());
                                    if (snapshotState.isIncrementalSnapshot()) {
                                        snapshotStateList.putSnapshotState(
                                                incrementalStateByTimeEntry.getKey(),
                                                snapshotState);
                                    } else {
                                        snapshotStateList.getSnapshotStates().clear();
                                        snapshotStateList.putSnapshotState(
                                                incrementalStateByTimeEntry.getKey(),
                                                snapshotState);
                                    }
                                } else {
                                    deserializedElementStateMap.put(singleIncrementSnapshotEntry.getKey(),
                                            singleIncrementSnapshotEntry.getValue());
                                }
                            }
                        }
                    }
                }
                snapshotable.restoreState(deserializedElementStateMap);
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
                        int results = o1.getElementId().compareTo(o2.getElementId());
                        if (results == 0) {
                            results = Long.compare(o2.getTime(), o1.getTime());
                            if (results == 0) {
                                return o2.getType().compareTo(o1.getType());
                            }
                        }
                        return results;
                    }
                });
                String lastElementId = null;
                boolean baseFound = false;
                boolean perioicFound = false;
                for (Iterator<IncrementalSnapshotInfo> iterator = incrementalSnapshotInfos.iterator();
                     iterator.hasNext(); ) {
                    IncrementalSnapshotInfo snapshotInfo = iterator.next();
                    if (snapshotInfo.getElementId().equals(lastElementId)) {
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
                    lastElementId = snapshotInfo.getElementId();
                }
                Map<String, Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>>> incrementalState =
                        new HashMap<>();
                for (IncrementalSnapshotInfo snapshotInfo : incrementalSnapshotInfos) {
                    Map<String, Map<Long, Map<IncrementalSnapshotInfo, byte[]>>> incrementalStateByElementId =
                            incrementalState.computeIfAbsent(snapshotInfo.getQueryName(), k -> new TreeMap<>());
                    Map<Long, Map<IncrementalSnapshotInfo, byte[]>> incrementalStateByTime =
                            incrementalStateByElementId.computeIfAbsent(snapshotInfo.getElementId(),
                                    k -> new TreeMap<>());
                    Map<IncrementalSnapshotInfo, byte[]> incrementalStateByInfo = incrementalStateByTime.
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
}

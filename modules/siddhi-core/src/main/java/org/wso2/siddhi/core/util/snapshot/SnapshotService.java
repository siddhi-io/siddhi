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
import org.wso2.siddhi.core.util.ThreadBarrier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Service level implementation to take/restore snapshots of processing elements.
 */
public class SnapshotService {
    private static final Logger log = Logger.getLogger(SnapshotService.class);
    private static final ThreadLocal<Boolean> skipSnapshotableThreadLocal = new ThreadLocal<Boolean>();

    private final ThreadBarrier threadBarrier;
    private HashMap<String, List<Snapshotable>> snapshotableMap = new HashMap<String, List<Snapshotable>>();
    private SiddhiAppContext siddhiAppContext;

    public SnapshotService(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.threadBarrier = siddhiAppContext.getThreadBarrier();

    }

    public static ThreadLocal<Boolean> getSkipSnapshotableThreadLocal() {
        return skipSnapshotableThreadLocal;
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

    public SnapshotSerialized snapshot() {
        HashMap<String, Object> elementWiseIncrementalSnapshots;
        HashMap<String, Object> elementWiseIncrementalSnapshotsBase = new HashMap<>();
        HashMap<String, Object> elementWiseFullSnapshots = new HashMap<>();
        HashMap<String, HashMap<String, Object>> elementSnapshotMapFull = new HashMap<>();
        HashMap<String, HashMap<String, Object>> elementSnapshotMapIncremental = new HashMap<>();
        HashMap<String, HashMap<String, Object>> elementSnapshotMapIncrementalBase = new HashMap<>();
        byte[] serializedSnapshots;

        if (log.isDebugEnabled()) {
            log.debug("Taking snapshot ...");
        }

        try {
            threadBarrier.lock();

            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                elementWiseIncrementalSnapshots = new HashMap<>();

                Iterator<Snapshotable> iterator = (Iterator<Snapshotable>) entry.getValue().iterator();
                while (iterator.hasNext()) {
                    Snapshotable object = iterator.next();
                    HashMap<String, Object> currentState = (HashMap<String, Object>) object.currentState();
                    if (currentState != null) {
                        Map<String, Object> incrementalSnapshotableMap = new HashMap<String, Object>();
                        Map<String, Object> incrementalSnapshotableMapBase = new HashMap<String, Object>();
                        HashMap<String, Object> elementWiseSnapshots = new HashMap<>();

                        for (Map.Entry<String, Object> item2: currentState.entrySet()) {
                            String key = item2.getKey();
                            Object snapShot = item2.getValue();

                            if (snapShot instanceof Snapshot) {
                                if (((Snapshot) snapShot).getState() != null) {
                                    if (((Snapshot) snapShot).isIncrementalSnapshot()) {
                                        incrementalSnapshotableMapBase.put(key, snapShot);
                                    } else {
                                        incrementalSnapshotableMap.put(key, snapShot);
                                    }
                                }
                            } else {
                                elementWiseSnapshots.put(key, snapShot);
                            }

                            if (!incrementalSnapshotableMap.isEmpty()) {
                                //Do we need to get and then update?
                                elementWiseIncrementalSnapshots.put(object.getElementId(),
                                        ByteSerializer.objectToByte(incrementalSnapshotableMap, siddhiAppContext));
                            }

                            if (!incrementalSnapshotableMapBase.isEmpty()) {
                                elementWiseIncrementalSnapshotsBase.put(object.getElementId(),
                                        ByteSerializer.objectToByte(incrementalSnapshotableMapBase, siddhiAppContext));
                            }

                            if (!elementWiseSnapshots.isEmpty()) {
                                elementWiseFullSnapshots.put(object.getElementId(), elementWiseSnapshots);
                            }
                        }
                    }
                }

                if (!elementWiseIncrementalSnapshots.isEmpty()) {
                    elementSnapshotMapIncremental.put(entry.getKey(), elementWiseIncrementalSnapshots);
                }

                if (!elementWiseFullSnapshots.isEmpty()) {
                    elementSnapshotMapFull.put(entry.getKey(), elementWiseFullSnapshots);
                }

                if (!elementWiseIncrementalSnapshotsBase.isEmpty()) {
                    elementSnapshotMapIncrementalBase.put(entry.getKey(), elementWiseIncrementalSnapshotsBase);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Snapshot serialization started ...");
            }
            serializedSnapshots = ByteSerializer.objectToByte(elementSnapshotMapFull, siddhiAppContext);
            if (log.isDebugEnabled()) {
                log.debug("Snapshot serialization finished.");
            }
        } finally {
            threadBarrier.unlock();
        }
        if (log.isDebugEnabled()) {
            log.debug("Snapshot taken for Siddhi app '" + siddhiAppContext.getName() + "'");
        }

        SnapshotSerialized result = new SnapshotSerialized();
        result.fullState = serializedSnapshots;

        if (!elementSnapshotMapIncremental.isEmpty()) {
            result.incrementalState = elementSnapshotMapIncremental;
        }

        if (!elementSnapshotMapIncrementalBase.isEmpty()) {
            result.incrementalStateBase = elementSnapshotMapIncrementalBase;
        }

        return result;
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

    public void restore(Map<String, Map<String, Object>> snapshots) throws CannotRestoreSiddhiAppStateException {
        List<Snapshotable> snapshotableList;
        try {
            threadBarrier.lock();
            List<Snapshotable> partitionSnapshotables = snapshotableMap.get("partition");
            try {
                if (partitionSnapshotables != null) {
                    for (Snapshotable snapshotable : partitionSnapshotables) {
                        snapshotable.restoreState(snapshots.get(snapshotable.getElementId()));
                    }
                }
            } catch (Throwable t) {
                throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                        getName() + " not completed properly because content of Siddhi app has changed since " +
                        "last state persistence. Clean persistence store for a fresh deployment.", t);
            }

            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                if (entry.getKey().equals("partition")) {
                    continue;
                }
                snapshotableList = entry.getValue();
                try {
                    for (Snapshotable snapshotable : snapshotableList) {
                        HashMap<String, Object> hmap = (HashMap<String, Object>) snapshots.get(entry.getKey());
                        if (hmap != null) {
                            HashMap<String, Object> variablesForElement = (HashMap<String, Object>)
                                    hmap.get(snapshotable.getElementId());

                            if (variablesForElement != null) {
                                snapshotable.restoreState(variablesForElement);
                            }
                        }
                    }
                } catch (Throwable t) {
                    throw new CannotRestoreSiddhiAppStateException("Restoring of Siddhi app " + siddhiAppContext.
                            getName() + " not completed properly because content of Siddhi app has changed since " +
                            "last state persistence. Clean persistence store for a fresh deployment.", t);
                }
            }
        } finally {
            threadBarrier.unlock();
        }
    }

    public HashMap<String, Object> recoverFromIncrementalSnapshots(String variableName,
                                                                   HashMap<String, Object> snapshots) {
        TreeSet<Long> revisions = new TreeSet<Long>();
        for (Map.Entry<String, Object> entry : snapshots.entrySet()) {
            long item = -1L;
            try {
                item = Long.parseLong(entry.getKey());
                revisions.add(item);
            } catch (NumberFormatException e) {
                //ignore
            }
        }

        Iterator<Long> itr = revisions.iterator();
        boolean firstFlag = true;
        Snapshot snpObj;

        while (itr.hasNext()) {
            HashMap<String, Snapshot> obj = (HashMap<String, Snapshot>) snapshots.get("" + itr.next());

            for (Map.Entry<String, Snapshot> item: obj.entrySet()) {

                if (firstFlag) {
                    snpObj = (Snapshot) item.getValue();
                    firstFlag = false;
                } else {
                    Snapshot snpObj2 = (Snapshot) item.getValue();

                    if (snpObj2.isIncrementalSnapshot()) {

                    } else {
                        snpObj = snpObj2;
                    }
                }
            }
        }

        return null;
    }
}

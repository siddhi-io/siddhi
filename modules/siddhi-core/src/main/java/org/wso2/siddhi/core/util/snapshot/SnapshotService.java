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

import java.util.*;

/**
 * Service level implementation to take/restore snapshots of processing elements.
 */
public class SnapshotService {


    private static final Logger log = Logger.getLogger(SnapshotService.class);
    private final ThreadBarrier threadBarrier;
    private HashMap<String, List<Snapshotable>> snapshotableMap = new HashMap<String, List<Snapshotable>>();
    private SiddhiAppContext siddhiAppContext;

    public SnapshotService(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.threadBarrier = siddhiAppContext.getThreadBarrier();

    }

    public synchronized void addSnapshotable(String queryName, Snapshotable snapshotable) {

        List<Snapshotable> snapshotableList = snapshotableMap.get(queryName);

        // if List does not exist create it
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

    public SnapshotSerialized snapshot() {
        HashMap<String, Object> elementWiseIncrementalSnapshots = new HashMap<>();
        HashMap<String, Object> elementWiseFullSnapshots = new HashMap<>();
        HashMap<String, HashMap<String, Object>> elementSnapshotMapFull = new HashMap<>();
        HashMap<String, HashMap<String, Object>> elementSnapshotMapIncremental = new HashMap<>();
        byte[] serializedSnapshots;

        if (log.isDebugEnabled()) {
            log.debug("Taking snapshot ...");
        }

        try {
            threadBarrier.lock();

            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                elementWiseIncrementalSnapshots = new HashMap<>();

                Iterator<Snapshotable> iterator = (Iterator<Snapshotable>) entry.getValue().iterator();
                while(iterator.hasNext()){
                    Snapshotable object = iterator.next();
                    HashMap<String, Object> currentState = (HashMap<String, Object>) object.currentState();
                    if(currentState != null) {
                        Set<String> set = (Set<String>) currentState.keySet();
                        Iterator<String> itr2 = set.iterator();
                        Map<String, Object> incrementalSnapshotableMap = new HashMap<String, Object>();
                        HashMap<String, Object> elementWiseSnapshots = new HashMap<>();

                        while (itr2.hasNext()) {
                            String key = itr2.next();
                            if (key.contains("inc-")) {
                                incrementalSnapshotableMap.put(key, currentState.get(key));
                            } else {
                                elementWiseSnapshots.put(key, currentState.get(key));
                            }
                        }

                        if(!incrementalSnapshotableMap.isEmpty()) {
                            //Do we need to get and then update?
                            elementWiseIncrementalSnapshots.put(object.getElementId(),
                                    ByteSerializer.objectToByte(incrementalSnapshotableMap, siddhiAppContext));
                        }

                        if(!elementWiseSnapshots.isEmpty()) {
                            elementWiseFullSnapshots.put(object.getElementId(), elementWiseSnapshots);
                        }
                    }
                }

                if(!elementWiseIncrementalSnapshots.isEmpty()) {
                    elementSnapshotMapIncremental.put(entry.getKey(), elementWiseIncrementalSnapshots);
                }

                if(!elementWiseFullSnapshots.isEmpty()){
                    elementSnapshotMapFull.put(entry.getKey(), elementWiseFullSnapshots);
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

        if(!elementWiseIncrementalSnapshots.isEmpty()) {
            result.incrementalState = elementSnapshotMapIncremental;
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
            for (Map.Entry<String, List<Snapshotable>> entry : snapshotableMap.entrySet()) {
                snapshotableList = entry.getValue();
                try {
                    for (Snapshotable snapshotable : snapshotableList) {
                        HashMap<String, Object> hmap = (HashMap<String, Object>)snapshots.get(entry.getKey());
                        snapshotable.restoreState((HashMap<String, Object>)hmap.get(snapshotable.getElementId()));
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

}

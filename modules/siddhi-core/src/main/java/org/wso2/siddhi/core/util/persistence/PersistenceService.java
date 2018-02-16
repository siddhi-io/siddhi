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
package org.wso2.siddhi.core.util.persistence;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.util.snapshot.ByteSerializer;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Persistence Service is the service layer to handle state persistence tasks such as persisting current state and
 * restoring previous states.
 */
public class PersistenceService {

    private static final Logger log = Logger.getLogger(PersistenceService.class);
    private String siddhiAppName;
    private PersistenceStore persistenceStore;
    private IncrementalPersistenceStore incrementalPersistanceStore;
    private SnapshotService snapshotService;
    private SiddhiAppContext context;

    public PersistenceService(SiddhiAppContext siddhiAppContext) {
        this.snapshotService = siddhiAppContext.getSnapshotService();
        this.persistenceStore = siddhiAppContext.getSiddhiContext().getPersistenceStore();
        this.incrementalPersistanceStore = siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore();
        this.siddhiAppName = siddhiAppContext.getName();
        this.context = siddhiAppContext;
    }

    public String persist() {

        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Persisting...");
            }
            byte[] snapshot = snapshotService.snapshot().fullState;
            String revision = System.currentTimeMillis() + "_" + siddhiAppName;
            persistenceStore.save(siddhiAppName, revision, snapshot);

            if (log.isDebugEnabled()) {
                log.debug("Persisted.");
            }
            return revision;
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }

    }

    public void restoreRevision(String revision) throws CannotRestoreSiddhiAppStateException {
        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Restoring revision: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(siddhiAppName, revision);

            //First, load the full snapshot
            HashMap<String, Map<String, Object>> snapshots = (HashMap<String, Map<String, Object>>)
                    ByteSerializer.byteToObject(snapshot, context);

            //Next, load the incremental state

            if (incrementalPersistanceStore != null) {
                HashMap<String, Object> hmap1;
                HashMap<String, Object> hmap2;
                ArrayList<ArrayList<String>> list = incrementalPersistanceStore.getListOfRevisionsToLoad(siddhiAppName);

                for (ArrayList<String> element : list) {
                    byte[] item = incrementalPersistanceStore.load(element.get(1), element.get(2),
                            element.get(3), element.get(0), element.get(4));

                    hmap2 = (HashMap<String, Object>) snapshots.get(element.get(2));

                    if (hmap2 == null) {
                        hmap2 = new HashMap<>();
                    }

                    hmap1 = (HashMap<String, Object>) hmap2.get(element.get(3));

                    if (hmap1 == null) {
                        hmap1 = new HashMap<>();
                    }

                    hmap1.put(element.get(0), (HashMap<String, Object>) ByteSerializer.byteToObject(
                            item, context));
                    hmap2.put(element.get(3), hmap1);

                    snapshots.put(element.get(2), hmap2);
                }
            }

            snapshotService.restore(snapshots);
            if (log.isDebugEnabled()) {
                log.debug("Restored revision: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }
    }

    public String restoreLastRevision() throws CannotRestoreSiddhiAppStateException {
        if (persistenceStore != null) {
            String revision = persistenceStore.getLastRevision(siddhiAppName);
            if (revision != null) {
                restoreRevision(revision);
            }
            return revision;
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }
    }

    //ToDO:Need to identify where this method has been used.
    public void restore(byte[] snapshot) throws CannotRestoreSiddhiAppStateException {
        //snapshotService.restore(snapshot);
    }
}

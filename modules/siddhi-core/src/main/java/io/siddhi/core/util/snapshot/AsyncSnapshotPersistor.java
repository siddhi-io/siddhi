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

package io.siddhi.core.util.snapshot;

import io.siddhi.core.exception.NoPersistenceStoreException;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.core.util.persistence.util.PersistenceConstants;
import org.apache.log4j.Logger;

/**
 * {@link Runnable} which is responsible for persisting the snapshots that are taken
 */
public class AsyncSnapshotPersistor implements Runnable {
    private static final Logger log = Logger.getLogger(AsyncSnapshotPersistor.class);
    private byte[] snapshots;
    private PersistenceStore persistenceStore;
    private String siddhiAppName;
    private String revision;
    private long time;

    public AsyncSnapshotPersistor(byte[] snapshots, PersistenceStore persistenceStore,
                                  String siddhiAppName, long time) {
        if (persistenceStore == null) {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app '" +
                    siddhiAppName + "'");
        }
        this.snapshots = snapshots;
        this.persistenceStore = persistenceStore;
        this.siddhiAppName = siddhiAppName;
        this.time = time;
        this.revision = time + PersistenceConstants.REVISION_SEPARATOR + siddhiAppName;
    }

    public String getRevision() {
        return revision;
    }

    @Override
    public void run() {
        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Persisting...");
            }
            persistenceStore.save(siddhiAppName, revision, snapshots);
            if (log.isDebugEnabled()) {
                log.debug("Persisted.");
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }

    }
}

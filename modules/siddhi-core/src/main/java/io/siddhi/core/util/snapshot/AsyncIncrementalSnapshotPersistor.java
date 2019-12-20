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
import io.siddhi.core.util.persistence.IncrementalPersistenceStore;
import io.siddhi.core.util.persistence.util.IncrementalSnapshotInfo;
import org.apache.log4j.Logger;

/**
 * {@link Runnable} which is responsible for persisting the snapshots that are taken
 */
public class AsyncIncrementalSnapshotPersistor implements Runnable {
    private static final Logger log = Logger.getLogger(AsyncIncrementalSnapshotPersistor.class);
    private byte[] snapshots;
    private IncrementalPersistenceStore incrementalPersistenceStore;
    private IncrementalSnapshotInfo snapshotInfo;

    public AsyncIncrementalSnapshotPersistor(byte[] snapshots, IncrementalPersistenceStore incrementalPersistenceStore,
                                             IncrementalSnapshotInfo snapshotInfo) {
        if (incrementalPersistenceStore == null) {
            throw new NoPersistenceStoreException("No incremental persistence store assigned for siddhi app '" +
                    snapshotInfo.getSiddhiAppId() + "'");
        }
        this.snapshots = snapshots;
        this.incrementalPersistenceStore = incrementalPersistenceStore;
        this.snapshotInfo = snapshotInfo;
    }

    public String getRevision() {
        return snapshotInfo.getRevision();
    }

    @Override
    public void run() {
        if (incrementalPersistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Persisting...");
            }
            incrementalPersistenceStore.save(snapshotInfo, snapshots);
            if (log.isDebugEnabled()) {
                log.debug("Persisted.");
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    snapshotInfo.getSiddhiAppId());
        }

    }
}

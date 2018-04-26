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

package org.wso2.siddhi.core.util.snapshot;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.util.persistence.IncrementalPersistenceStore;

/**
 * {@link Runnable} which is responsible for persisting the snapshots that are taken
 */
public class AsyncIncrementalSnapshotPersistor implements Runnable {
    private static final Logger log = Logger.getLogger(AsyncIncrementalSnapshotPersistor.class);
    private byte[] snapshots;
    private IncrementalPersistenceStore persistenceStore;
    private String revision;
    private String siddhiAppName;
    private String queryName;
    private String elementID;
    private String type;

    public AsyncIncrementalSnapshotPersistor(byte[] snapshots, IncrementalPersistenceStore persistenceStore,
                                             String siddhiAppName, String queryName, String elementID,
                                             String revisionToUse,
                                             String type) {
        this.snapshots = snapshots;
        this.persistenceStore = persistenceStore;
        this.siddhiAppName = siddhiAppName;
        this.queryName = queryName;
        this.elementID = elementID;
        this.type = type;
        revision = revisionToUse + "_" + siddhiAppName + "_" + queryName + "_" + elementID + "_" + type;
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

            persistenceStore.save(siddhiAppName, queryName, elementID, revision, type, snapshots);

            if (log.isDebugEnabled()) {
                log.debug("Persisted.");
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for siddhi app " +
                    siddhiAppName);
        }

    }
}

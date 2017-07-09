/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.util.ThreadBarrier;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;

public class PersistenceService {

    private static final Logger LOGGER = Logger.getLogger(PersistenceService.class);
    private static final String RECEIVER_SUFFIX = ".receiver";
    private String executionPlanName;
    private PersistenceStore persistenceStore;
    private SnapshotService snapshotService;
    private ThreadBarrier threadBarrier;

    public PersistenceService(ExecutionPlanContext executionPlanContext) {
        this.snapshotService = executionPlanContext.getSnapshotService();
        this.persistenceStore = executionPlanContext.getSiddhiContext().getPersistenceStore();
        this.executionPlanName = executionPlanContext.getName();
        this.threadBarrier = executionPlanContext.getThreadBarrier();
    }

    public String persist() {
        if (persistenceStore != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Persisting...");
            }
            byte[] snapshot = snapshotService.snapshot();
            byte[] receiversSnapshot = snapshotService.snapshotReceivers();
            String revision = System.currentTimeMillis() + "_" + executionPlanName;
            persistenceStore.save(executionPlanName, revision, snapshot);
            persistenceStore.save(executionPlanName + RECEIVER_SUFFIX, revision, receiversSnapshot);
            snapshotService.nofityReceiversOnSave(receiversSnapshot);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Persisted.");
            }
            return revision;
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }
    }

    public void restoreRevision(String revision) {
        if (persistenceStore != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Restoring revision: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(executionPlanName, revision);
            snapshotService.restore(snapshot);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Restored revision: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }
    }

    public void restoreReceiversRevision(String revision) {
        if (persistenceStore != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Restoring receivers revision: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(executionPlanName + RECEIVER_SUFFIX, revision);
            snapshotService.restoreReceivers(snapshot);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Restored receivers revision: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }
    }

    public void restoreLastRevision() {
        try {
            this.threadBarrier.lock();
            if (persistenceStore != null) {
                String revision = persistenceStore.getLastRevision(executionPlanName);
                String receiversRevision = persistenceStore.getLastRevision(executionPlanName + RECEIVER_SUFFIX);
                if (revision != null) {
                    restoreRevision(revision);
                }
                if (receiversRevision != null) {
                    restoreReceiversRevision(receiversRevision);
                }
            } else {
                throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
            }
        } finally {
            threadBarrier.unlock();
        }
    }
}

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

package io.siddhi.core.util.persistence;

import com.google.common.io.Files;
import io.siddhi.core.exception.CannotClearSiddhiAppStateException;
import io.siddhi.core.util.persistence.util.IncrementalSnapshotInfo;
import io.siddhi.core.util.persistence.util.PersistenceConstants;
import io.siddhi.core.util.persistence.util.PersistenceHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Persistence Store that would persist snapshots to the file system
 */
public class IncrementalFileSystemPersistenceStore implements IncrementalPersistenceStore {

    private static final Logger log = LogManager.getLogger(IncrementalFileSystemPersistenceStore.class);
    private String folder;

    public IncrementalFileSystemPersistenceStore() {
    }

    public IncrementalFileSystemPersistenceStore(String storageFilePath) {
        folder = storageFilePath;
    }

    @Override
    public void save(IncrementalSnapshotInfo snapshotInfo, byte[] snapshot) {
        File file = new File(folder + File.separator + snapshotInfo.getSiddhiAppId() + File.separator +
                snapshotInfo.getRevision());
        try {
            Files.createParentDirs(file);
            Files.write(snapshot, file);
            cleanOldRevisions(snapshotInfo);
            if (log.isDebugEnabled()) {
                log.debug("Incremental persistence of '" + snapshotInfo.getSiddhiAppId() +
                        "' with revision '" + snapshotInfo.getRevision() + "' persisted successfully.");
            }
        } catch (IOException e) {
            log.error("Cannot save the revision '" + snapshotInfo.getRevision() + "' of SiddhiApp: '" +
                    snapshotInfo.getSiddhiAppId() + "' to the file system.", e);
        }
    }

    @Override
    public void setProperties(Map properties) {
        //nothing to do
    }

    @Override
    public byte[] load(IncrementalSnapshotInfo snapshotInfo) {
        File file = new File(folder + File.separator + snapshotInfo.getSiddhiAppId() + File.separator +
                snapshotInfo.getRevision());
        byte[] bytes = null;
        try {
            bytes = Files.toByteArray(file);
            if (log.isDebugEnabled()) {
                log.debug("State loaded for SiddhiApp '" + snapshotInfo.getSiddhiAppId() + "' revision '" +
                        snapshotInfo.getRevision() + "' from file system.");
            }
        } catch (IOException e) {
            log.error("Cannot load the revision '" + snapshotInfo.getRevision() + "' of SiddhiApp '" +
                    snapshotInfo.getSiddhiAppId() + "' from file system.", e);
        }
        return bytes;
    }

    @Override
    public List<IncrementalSnapshotInfo> getListOfRevisionsToLoad(long restoreTime, String siddhiAppName) {

        File dir = new File(folder + File.separator + siddhiAppName);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        List<IncrementalSnapshotInfo> results = new ArrayList<>();
        for (File file : files) {
            String fileName = file.getName();
            IncrementalSnapshotInfo snapshotInfo = PersistenceHelper.convertRevision(fileName);
            if (snapshotInfo.getTime() <= restoreTime &&
                    siddhiAppName.equals(snapshotInfo.getSiddhiAppId()) &&
                    snapshotInfo.getId() != null &&
                    snapshotInfo.getQueryName() != null) {
                //Note: Here we discard the (items.length == 2) scenario which is handled
                // by the full snapshot handling
                if (log.isDebugEnabled()) {
                    log.debug("List of revisions to load : " + fileName);
                }
                results.add(snapshotInfo);
            }
        }
        return results;
    }

    @Override
    public String getLastRevision(String siddhiAppName) {
        long restoreTime = -1;
        IncrementalSnapshotInfo lastSnapshotInfo = null;
        File dir = new File(folder + File.separator + siddhiAppName);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        for (File file : files) {
            String fileName = file.getName();
            IncrementalSnapshotInfo snapshotInfo = PersistenceHelper.convertRevision(fileName);
            if (snapshotInfo.getTime() > restoreTime &&
                    siddhiAppName.equals(snapshotInfo.getSiddhiAppId()) &&
                    snapshotInfo.getId() != null &&
                    snapshotInfo.getQueryName() != null) {
                //Note: Here we discard the (items.length == 2) scenario which is handled
                // by the full snapshot handling
                restoreTime = snapshotInfo.getTime();
                lastSnapshotInfo = snapshotInfo;
            }
        }
        if (restoreTime != -1) {
            if (log.isDebugEnabled()) {
                log.debug("Latest revision to load: " + restoreTime + PersistenceConstants.REVISION_SEPARATOR +
                        siddhiAppName);
            }
            return lastSnapshotInfo.getRevision();
        }
        return null;
    }

    @Override
    public void clearAllRevisions(String siddhiAppName) {
        File dir = new File(folder + File.separator + siddhiAppName);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            log.info("No revisions were found to delete for the Siddhi App " + siddhiAppName);
            return;
        }

        for (File file : files) {
            if (file.exists()) {
                if (!file.delete()) {
                    log.error("file is not deleted successfully : " + file.getName());
                    throw new CannotClearSiddhiAppStateException("Persistence state " +
                            "file is not deleted : " + file.getName());
                }
            }

        }
    }

    private void cleanOldRevisions(IncrementalSnapshotInfo incrementalSnapshotInfo) {
        if (incrementalSnapshotInfo.getType() != IncrementalSnapshotInfo.SnapshotType.INCREMENT) {
            File dir = new File(folder + File.separator + incrementalSnapshotInfo.getSiddhiAppId());
            File[] files = dir.listFiles();
            if (files != null) {
                long baseTimeStamp = (incrementalSnapshotInfo.getTime());
                for (File file : files) {
                    String fileName = file.getName();
                    IncrementalSnapshotInfo snapshotInfo = PersistenceHelper.convertRevision(fileName);
                    if (snapshotInfo.getTime() < baseTimeStamp &&
                            incrementalSnapshotInfo.getId().equals(snapshotInfo.getId())) {
                        if (incrementalSnapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.BASE &&
                                snapshotInfo.getType() != IncrementalSnapshotInfo.SnapshotType.PERIODIC) {
                            if (file.exists()) {
                                Boolean isDeleted = file.delete();
                                if (!isDeleted) {
                                    log.error("Error deleting old revision " + fileName);
                                }
                            }
                        } else if (incrementalSnapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.PERIODIC &&
                                snapshotInfo.getType() == IncrementalSnapshotInfo.SnapshotType.PERIODIC) {
                            if (file.exists()) {
                                Boolean isDeleted = file.delete();
                                if (!isDeleted) {
                                    log.error("Error deleting old revision " + fileName);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

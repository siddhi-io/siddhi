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
import io.siddhi.core.util.persistence.util.PersistenceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Implementation of Persistence Store that would persist snapshots to the file system
 */
public class FileSystemPersistenceStore implements PersistenceStore {

    private static final Logger log = LogManager.getLogger(FileSystemPersistenceStore.class);
    private int numberOfRevisionsToSave;
    private String folder;

    @Override
    public void save(String siddhiAppName, String revision, byte[] snapshot) {
        File file = new File(folder + File.separator + siddhiAppName + File.separator + revision);
        try {
            Files.createParentDirs(file);
            Files.write(snapshot, file);
            cleanOldRevisions(siddhiAppName);
            if (log.isDebugEnabled()) {
                log.debug("Periodic persistence of " + siddhiAppName + " persisted successfully.");
            }
        } catch (IOException e) {
            log.error("Cannot save the revision " + revision + " of SiddhiApp: " + siddhiAppName +
                    " to the file system.", e);
        }
    }

    @Override
    public void setProperties(Map properties) {
        Map configurationMap = (Map) properties.get(PersistenceConstants.STATE_PERSISTENCE_CONFIGS);
        Object numberOfRevisionsObject = properties.get(PersistenceConstants.STATE_PERSISTENCE_REVISIONS_TO_KEEP);

        if (numberOfRevisionsObject == null || !(numberOfRevisionsObject instanceof Integer)) {
            numberOfRevisionsToSave = 3;
            if (log.isDebugEnabled()) {
                log.debug("Number of revisions to keep is not set or invalid. Default value will be used.");
            }
        } else {
            numberOfRevisionsToSave = Integer.parseInt(String.valueOf(numberOfRevisionsObject));
        }

        if (configurationMap != null) {
            Object folderObject = configurationMap.get("location");
            if (folderObject == null || !(folderObject instanceof String)) {
                folder = PersistenceConstants.DEFAULT_FILE_PERSISTENCE_FOLDER;
                if (log.isDebugEnabled()) {
                    log.debug("File system persistence location not set. Default persistence location will be used.");
                }
            } else {
                folder = String.valueOf(folderObject);
            }

        } else {
            folder = PersistenceConstants.DEFAULT_FILE_PERSISTENCE_FOLDER;
            if (log.isDebugEnabled()) {
                log.debug("File system persistence config not set. Default persistence location will be used.");
            }
        }
    }

    @Override
    public byte[] load(String siddhiAppName, String revision) {
        File file = new File(folder + File.separator + siddhiAppName + File.separator + revision);
        try {
            byte[] bytes = Files.toByteArray(file);
            log.info("State loaded for " + siddhiAppName + " revision " + revision + " from the file system.");
            return bytes;
        } catch (IOException e) {
            log.error("Cannot load the revision " + revision + " of SiddhiApp: " + siddhiAppName +
                    " from file system.", e);
        }
        return null;
    }

    @Override
    public String getLastRevision(String siddhiAppName) {
        File dir = new File(folder + File.separator + siddhiAppName);
        File[] files = dir.listFiles();

        if (files == null || files.length == 0) {
            return null;
        }

        String lastRevision = null;
        for (File file : files) {
            String fileName = file.getName();
            if (lastRevision == null || fileName.compareTo(lastRevision) > 0) {
                lastRevision = fileName;
            }
        }
        return lastRevision;
    }

    @Override
    public void clearAllRevisions(String siddhiAppName) {
        File targetDirectory = new File(folder + File.separator + siddhiAppName);
        File[] files = targetDirectory.listFiles();

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

    /**
     * Method to remove revisions that are older than the user specified amount
     *
     * @param siddhiAppName is the name of the Siddhi Application whose old revisions to remove
     */

    private void cleanOldRevisions(String siddhiAppName) {
        File targetDirectory = new File(folder + File.separator + siddhiAppName);
        File[] files = targetDirectory.listFiles();
        if (files != null) {
            while (files.length > numberOfRevisionsToSave) {
                String firstRevision = null;
                for (File file : files) {
                    String fileName = file.getName();
                    if (firstRevision == null || fileName.compareTo(firstRevision) < 0) {
                        firstRevision = fileName;
                    }
                }
                File fileToDelete = new File(targetDirectory + File.separator + firstRevision);
                if (fileToDelete.exists()) {
                    Boolean isDeleted = fileToDelete.delete();
                    if (!isDeleted) {
                        log.error("Error deleting old revision " + firstRevision);
                    }
                }
                files = targetDirectory.listFiles();
                if (files == null || files.length < 1) {
                    break;
                }
            }
        }
    }
}

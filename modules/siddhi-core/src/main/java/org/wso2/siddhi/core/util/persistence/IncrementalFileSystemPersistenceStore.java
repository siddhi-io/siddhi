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

package org.wso2.siddhi.core.util.persistence;

import com.google.common.io.Files;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.util.persistence.util.PersistenceConstants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Persistence Store that would persist snapshots to the file system
 */
public class IncrementalFileSystemPersistenceStore implements IncrementalPersistenceStore {

    private static final Logger log = Logger.getLogger(IncrementalFileSystemPersistenceStore.class);
    private String folder = "/home/miyurud/Desktop/temp";

    @Override
    public void save(String siddhiAppName, String queryName, String elementId, String revision, byte[] snapshot) {
        File file = new File(folder + File.separator + siddhiAppName + "_I" + File.separator + revision);
        try {
            Files.createParentDirs(file);
            Files.write(snapshot, file);

//            cleanOldRevisions(siddhiAppName);
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
    public HashMap<String, Object> load(String siddhiAppName, String queryName, String elementId, String revision) {
        File file = new File(folder + File.separator + siddhiAppName + "_I" + File.separator + revision + "_"
                + siddhiAppName + "_" + queryName + "_" + elementId + "_I");
        HashMap<String, Object> result = new HashMap<>();

        try {
            byte[] bytes = Files.toByteArray(file);
            log.info("State loaded for " + siddhiAppName + " revision " + revision + " from the file system.");
            result.put(elementId, bytes);
        } catch (IOException e) {
            log.error("Cannot load the revision " + revision + " of SiddhiApp: " + siddhiAppName +
                    " from file system.", e);
        }

        return result;
    }

    @Override
    public ArrayList<ArrayList<String>> getListOfRevisionsToLoad(String siddhiAppName) {
        File dir = new File(folder + File.separator + siddhiAppName + "_I");
        File[] files = dir.listFiles();
        ArrayList<ArrayList<String>> results = new ArrayList<>();

        if (files == null || files.length == 0) {
            return null;
        }

        for (File file : files) {
            String fileName = file.getName();
            if(fileName.contains(siddhiAppName)){
                ArrayList<String> result = new ArrayList<>();
                String[] items = fileName.split("_");

                result.add(items[0]);
                result.add(items[1]);
                result.add(items[2]);
                result.add(items[3]);
                results.add(result);
            }
        }

        return results;
    }
}

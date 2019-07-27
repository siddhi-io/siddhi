/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.persistence.util;

import io.siddhi.core.util.SiddhiConstants;

/**
 * Struct to store information about Incremental Snapshot
 */
public class IncrementalSnapshotInfo {

    private String id;
    private String siddhiAppId;
    private String partitionId;
    private String partitionGroupByKey;
    private String queryName;
    private String elementId;
    private long time;
    private String revision;
    private SnapshotType type;

    public IncrementalSnapshotInfo(String siddhiAppId, String partitionId, String queryName, String elementId,
                                   long time, SnapshotType type, String partitionGroupByKey) {
        this.siddhiAppId = siddhiAppId;
        this.partitionId = partitionId;
        this.queryName = queryName;
        this.elementId = elementId;
        this.time = time;
        this.type = type;
        this.partitionGroupByKey = partitionGroupByKey;
        this.id = (siddhiAppId +
                PersistenceConstants.REVISION_SEPARATOR + partitionId +
                PersistenceConstants.REVISION_SEPARATOR + partitionGroupByKey +
                PersistenceConstants.REVISION_SEPARATOR + queryName +
                PersistenceConstants.REVISION_SEPARATOR + elementId).
                replaceAll(SiddhiConstants.KEY_DELIMITER, SiddhiConstants.KEY_DELIMITER_FILE);
        this.revision = (time + PersistenceConstants.REVISION_SEPARATOR + siddhiAppId +
                PersistenceConstants.REVISION_SEPARATOR + partitionId +
                PersistenceConstants.REVISION_SEPARATOR + partitionGroupByKey +
                PersistenceConstants.REVISION_SEPARATOR + queryName +
                PersistenceConstants.REVISION_SEPARATOR + elementId +
                PersistenceConstants.REVISION_SEPARATOR + type).
                replaceAll(SiddhiConstants.KEY_DELIMITER, SiddhiConstants.KEY_DELIMITER_FILE);
    }

    public String getSiddhiAppId() {
        return siddhiAppId;
    }

    public void setSiddhiAppId(String siddhiAppId) {
        this.siddhiAppId = siddhiAppId;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public String getRevision() {
        return revision;
    }

    public SnapshotType getType() {
        return type;
    }

    public void setType(SnapshotType type) {
        this.type = type;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getPartitionGroupByKey() {
        return partitionGroupByKey;
    }

    public String getId() {
        return id;
    }

    /**
     * Type of incremental snapshot types
     */
    public enum SnapshotType {
        PERIODIC, BASE, INCREMENT
    }
}

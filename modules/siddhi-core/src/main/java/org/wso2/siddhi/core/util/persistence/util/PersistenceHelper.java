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

package org.wso2.siddhi.core.util.persistence.util;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.PersistenceStoreException;
import org.wso2.siddhi.core.util.snapshot.AsyncIncrementalSnapshotPersistor;
import org.wso2.siddhi.core.util.snapshot.AsyncSnapshotPersistor;
import org.wso2.siddhi.core.util.snapshot.IncrementalSnapshot;
import org.wso2.siddhi.core.util.snapshot.PersistenceReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Helper Class to persist snapshots
 */
public final class PersistenceHelper {

    public static IncrementalSnapshotInfo convertRevision(String revision) {
        String[] items = revision.split(PersistenceConstants.REVISION_SEPARATOR);
        //Note: Here we discard the (items.length == 2) scenario which is handled by the full snapshot handling
        if (items.length == 5) {
            return new IncrementalSnapshotInfo(items[1], items[2], items[3],
                    Long.parseLong(items[0]), IncrementalSnapshotInfo.SnapshotType.valueOf(items[4]));
        } else if (items.length == 2) {
            return new IncrementalSnapshotInfo(items[1], null, null,
                    Long.parseLong(items[0]), IncrementalSnapshotInfo.SnapshotType.PERIODIC);
        } else {
            throw new PersistenceStoreException("Invalid revision found '" + revision + "'!");
        }
    }

    public static PersistenceReference persist(byte[] serializeObj, SiddhiAppContext siddhiAppContext) {
        long revisionTime = System.currentTimeMillis();
        // start the snapshot persisting task asynchronously
        AsyncSnapshotPersistor asyncSnapshotPersistor = new AsyncSnapshotPersistor(serializeObj,
                siddhiAppContext.getSiddhiContext().getPersistenceStore(), siddhiAppContext.getName(),
                revisionTime);
        Future future = siddhiAppContext.getExecutorService().submit(asyncSnapshotPersistor);
        return new PersistenceReference(future, asyncSnapshotPersistor.getRevision());
    }

    public static PersistenceReference persist(IncrementalSnapshot serializeObj, SiddhiAppContext siddhiAppContext) {
        long revisionTime = System.currentTimeMillis();
        List<Future> incrementalFutures = new ArrayList<>();
        //Periodic state
        Map<String, Map<String, byte[]>> periodicStateBase = serializeObj.getPeriodicState();
        if (periodicStateBase != null) {
            periodicStateBase.forEach((queryName, value) -> {
                value.forEach((elementId, value1) -> {
                    AsyncIncrementalSnapshotPersistor asyncIncrementSnapshotPersistor = new
                            AsyncIncrementalSnapshotPersistor(value1,
                            siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore(),
                            new IncrementalSnapshotInfo(siddhiAppContext.getName(), queryName, elementId,
                                    revisionTime, IncrementalSnapshotInfo.SnapshotType.PERIODIC));
                    Future future = siddhiAppContext.getExecutorService().
                            submit(asyncIncrementSnapshotPersistor);
                    incrementalFutures.add(future);
                });
            });
        }
        //Incremental base state
        Map<String, Map<String, byte[]>> incrementalStateBase = serializeObj.getIncrementalStateBase();
        if (incrementalStateBase != null) {
            incrementalStateBase.forEach((queryName, value) -> {
                value.forEach((elementId, value1) -> {
                    AsyncIncrementalSnapshotPersistor asyncIncrementSnapshotPersistor = new
                            AsyncIncrementalSnapshotPersistor(value1,
                            siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore(),
                            new IncrementalSnapshotInfo(siddhiAppContext.getName(), queryName, elementId,
                                    revisionTime, IncrementalSnapshotInfo.SnapshotType.BASE));
                    Future future = siddhiAppContext.getExecutorService().
                            submit(asyncIncrementSnapshotPersistor);
                    incrementalFutures.add(future);
                });
            });
        }
        //Next, handle the increment persistence scenarios
        //Incremental state
        Map<String, Map<String, byte[]>> incrementalState = serializeObj.getIncrementalState();
        if (incrementalState != null) {
            incrementalState.forEach((queryName, value) -> {
                value.forEach((elementId, value1) -> {
                    AsyncIncrementalSnapshotPersistor asyncIncrementSnapshotPersistor = new
                            AsyncIncrementalSnapshotPersistor(value1,
                            siddhiAppContext.getSiddhiContext().getIncrementalPersistenceStore(),
                            new IncrementalSnapshotInfo(siddhiAppContext.getName(), queryName, elementId,
                                    revisionTime, IncrementalSnapshotInfo.SnapshotType.INCREMENT));
                    Future future = siddhiAppContext.getExecutorService().
                            submit(asyncIncrementSnapshotPersistor);
                    incrementalFutures.add(future);
                });
            });
        }
        return new PersistenceReference(incrementalFutures,
                revisionTime + PersistenceConstants.REVISION_SEPARATOR + siddhiAppContext.getName());
    }
}

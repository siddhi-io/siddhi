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

import java.util.List;
import java.util.concurrent.Future;

/**
 * Class which contains the references related to persistence
 */
public class PersistenceReference {

    private Future fullStateFuture;
    private List<Future> incrementalStateFuture;
    private String revision;

    public PersistenceReference(Future fullStateFuture, String revision) {
        this.fullStateFuture = fullStateFuture;
        this.revision = revision;
    }

    public PersistenceReference(List<Future> incrementalStateFuture, String revision) {
        this.incrementalStateFuture = incrementalStateFuture;
        this.revision = revision;
    }

    @Deprecated
    public Future getFuture() {
        return fullStateFuture;
    }

    public String getRevision() {
        return revision;
    }

    public List<Future> getIncrementalStateFuture() {
        return incrementalStateFuture;
    }

    public Future getFullStateFuture() {
        return fullStateFuture;
    }
}

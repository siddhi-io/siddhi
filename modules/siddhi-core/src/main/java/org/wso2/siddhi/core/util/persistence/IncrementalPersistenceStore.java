/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Interface class for Persistence Stores which does incremental checkpointing.
 */
public interface IncrementalPersistenceStore {

    void save(String siddhiAppId, String queryName, String elementID, String revision, byte[] snapshot, String type);

    void setProperties(Map properties);

    HashMap<String, Object> load(String siddhiAppId, String queryName, String elementID, String revision, String type);

    ArrayList<ArrayList<String>> getListOfRevisionsToLoad(String siddhiAppId);
}

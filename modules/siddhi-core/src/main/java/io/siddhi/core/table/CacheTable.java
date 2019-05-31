/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.table;

import io.siddhi.core.table.holder.IndexEventHolder;

/**
 * Extension of InMemoryTable to be used as cache for store tables. This has methods for cache policies
 */
public class CacheTable extends InMemoryTable {
    private String cachePolicy;

    public void setCachePolicy(String cachePolicy) {
        this.cachePolicy = cachePolicy;
    }

    public void deleteOneEntryUsingCachePolicy() {
        if (cachePolicy.equals("FIFO")) {
            deleteOneEntryUsingFIFO();
        }
    }

    private void deleteOneEntryUsingFIFO() {
        try {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
            Object[] keys = indexEventHolder.getAllPrimaryKeyValues().toArray();

            long minTimestamp = Long.MAX_VALUE;
            Object keyOfMinTimestamp = null;

            for (Object key: keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                long timestamp = (long) data[data.length - 1];
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                    keyOfMinTimestamp = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinTimestamp);

        } catch (ClassCastException ignored) {

        }
    }
}

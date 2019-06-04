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
import io.siddhi.core.table.holder.IndexedEventHolder;

/**
 * cache table with FIFO entry removal
 */
public class CacheTableLFU extends InMemoryTable implements CacheTable {

    @Override
    public void deleteOneEntryUsingCachePolicy() {
        if (stateHolder.getState().eventHolder instanceof IndexedEventHolder) {
            IndexEventHolder indexEventHolder = (IndexEventHolder) stateHolder.getState().eventHolder;
            Object[] keys = indexEventHolder.getAllPrimaryKeyValues().toArray();

            int minCount = Integer.MAX_VALUE;
            Object keyOfMinCount = null;

            for (Object key : keys) {
                Object[] data = indexEventHolder.getEvent(key).getOutputData();
                int count = (int) data[data.length - 1];
                if (count < minCount) {
                    minCount = count;
                    keyOfMinCount = key;
                }
            }
            indexEventHolder.deleteEvent(keyOfMinCount);
        }
    }
}

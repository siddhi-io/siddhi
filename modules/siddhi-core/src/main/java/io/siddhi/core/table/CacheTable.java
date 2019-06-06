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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.table.record.RecordTableHandler;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;

/**
 * common interface for FIFO, LRU, and LFU cache tables
 */
public abstract class CacheTable extends InMemoryTable {

    public void initCacheTable(TableDefinition cacheTableDefinition, ConfigReader configReader,
                               SiddhiAppContext siddhiAppContext, RecordTableHandler recordTableHandler,
                               boolean cacheExpiryEnabled) {

        addRequiredFieldsToCacheTableDefinition(cacheTableDefinition, cacheExpiryEnabled);

        // initialize cache table
        MetaStreamEvent cacheTableMetaStreamEvent = new MetaStreamEvent();
        cacheTableMetaStreamEvent.addInputDefinition(cacheTableDefinition);
        for (Attribute attribute : cacheTableDefinition.getAttributeList()) {
            cacheTableMetaStreamEvent.addOutputData(attribute);
        }

        StreamEventFactory cacheTableStreamEventFactory = new StreamEventFactory(cacheTableMetaStreamEvent);
        StreamEventCloner cacheTableStreamEventCloner = new StreamEventCloner(cacheTableMetaStreamEvent,
                cacheTableStreamEventFactory);
        super.initTable(cacheTableDefinition, cacheTableStreamEventFactory,
                cacheTableStreamEventCloner, configReader, siddhiAppContext, recordTableHandler);
    }

    abstract void addRequiredFieldsToCacheTableDefinition(TableDefinition cacheTableDefinition,
                                                          boolean cacheExpiryEnabled);

    public abstract void deleteOneEntryUsingCachePolicy();
}

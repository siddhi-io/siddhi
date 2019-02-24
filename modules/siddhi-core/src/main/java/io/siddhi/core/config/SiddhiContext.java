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

package io.siddhi.core.config;

import com.lmax.disruptor.ExceptionHandler;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.PersistenceStoreException;
import io.siddhi.core.stream.input.source.SourceHandlerManager;
import io.siddhi.core.stream.output.sink.SinkHandlerManager;
import io.siddhi.core.table.record.RecordTableHandlerManager;
import io.siddhi.core.util.SiddhiExtensionLoader;
import io.siddhi.core.util.config.ConfigManager;
import io.siddhi.core.util.config.InMemoryConfigManager;
import io.siddhi.core.util.extension.holder.AbstractExtensionHolder;
import io.siddhi.core.util.persistence.IncrementalPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.core.util.statistics.metrics.SiddhiMetricsFactory;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Context information holder associated with {@link SiddhiManager}
 */
public class SiddhiContext {

    private static final Logger log = Logger.getLogger(SiddhiContext.class);

    private ExceptionHandler<Object> defaultDisrupterExceptionHandler;
    private Map<String, Class> siddhiExtensions = new HashMap<>();
    private PersistenceStore persistenceStore = null;
    private IncrementalPersistenceStore incrementalPersistenceStore = null;
    private ConcurrentHashMap<String, DataSource> siddhiDataSources;
    private StatisticsConfiguration statisticsConfiguration;
    private ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap;
    private ConfigManager configManager = null;
    private SinkHandlerManager sinkHandlerManager = null;
    private SourceHandlerManager sourceHandlerManager = null;
    private RecordTableHandlerManager recordTableHandlerManager = null;

    public SiddhiContext() {
        SiddhiExtensionLoader.loadSiddhiExtensions(siddhiExtensions);
        siddhiDataSources = new ConcurrentHashMap<String, DataSource>();
        statisticsConfiguration = new StatisticsConfiguration(new SiddhiMetricsFactory());
        extensionHolderMap = new ConcurrentHashMap<Class, AbstractExtensionHolder>();
        configManager = new InMemoryConfigManager();
        defaultDisrupterExceptionHandler = new ExceptionHandler<Object>() {
            @Override
            public void handleEventException(Throwable throwable, long l, Object event) {
                log.error("Disruptor encountered an error processing" + " [sequence: " + l + ", event: " + event
                        .toString() + "]", throwable);
            }

            @Override
            public void handleOnStartException(Throwable throwable) {
                log.error("Disruptor encountered an error on start", throwable);
            }

            @Override
            public void handleOnShutdownException(Throwable throwable) {
                log.error("Disruptor encountered an error on shutdown", throwable);
            }
        };
    }

    public Map<String, Class> getSiddhiExtensions() {
        return siddhiExtensions;
    }

    public synchronized PersistenceStore getPersistenceStore() {
        return persistenceStore;
    }

    public synchronized void setPersistenceStore(PersistenceStore persistenceStore) {
        if (incrementalPersistenceStore != null) {
            throw new PersistenceStoreException("Only one type of persistence store can exist. " +
                    "Incremental persistence store '" + incrementalPersistenceStore.getClass().getName() +
                    "' already registered!");
        }
        this.persistenceStore = persistenceStore;
    }

    public synchronized IncrementalPersistenceStore getIncrementalPersistenceStore() {
        return incrementalPersistenceStore;
    }

    public synchronized void setIncrementalPersistenceStore(IncrementalPersistenceStore incrementalPersistenceStore) {
        if (persistenceStore != null) {
            throw new PersistenceStoreException("Only one type of persistence store can exist." +
                    " Persistence store '" + persistenceStore.getClass().getName() + "' already registered!");
        }
        this.incrementalPersistenceStore = incrementalPersistenceStore;
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    public DataSource getSiddhiDataSource(String dataSourceName) {
        if (dataSourceName != null) {
            return siddhiDataSources.get(dataSourceName);
        }
        return null;
    }

    public void addSiddhiDataSource(String dataSourceName, DataSource dataSource) {
        this.siddhiDataSources.put(dataSourceName, dataSource);
    }

    public StatisticsConfiguration getStatisticsConfiguration() {
        return statisticsConfiguration;
    }

    public void setStatisticsConfiguration(StatisticsConfiguration statisticsConfiguration) {
        this.statisticsConfiguration = statisticsConfiguration;
    }

    public ConcurrentHashMap<Class, AbstractExtensionHolder> getExtensionHolderMap() {
        return extensionHolderMap;
    }

    public ExceptionHandler<Object> getDefaultDisrupterExceptionHandler() {
        return defaultDisrupterExceptionHandler;
    }

    public SinkHandlerManager getSinkHandlerManager() {
        return sinkHandlerManager;
    }

    public void setSinkHandlerManager(SinkHandlerManager sinkHandlerManager) {
        this.sinkHandlerManager = sinkHandlerManager;
    }

    public SourceHandlerManager getSourceHandlerManager() {
        return sourceHandlerManager;
    }

    public void setSourceHandlerManager(SourceHandlerManager sourceHandlerManager) {
        this.sourceHandlerManager = sourceHandlerManager;
    }

    public RecordTableHandlerManager getRecordTableHandlerManager() {
        return recordTableHandlerManager;
    }

    public void setRecordTableHandlerManager(RecordTableHandlerManager recordTableHandlerManager) {
        this.recordTableHandlerManager = recordTableHandlerManager;
    }

}

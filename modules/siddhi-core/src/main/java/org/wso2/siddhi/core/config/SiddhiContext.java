/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.config;

import com.lmax.disruptor.ExceptionHandler;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.SiddhiExtensionLoader;
import org.wso2.siddhi.core.util.extension.holder.AbstractExtensionHolder;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.core.util.statistics.metrics.SiddhiMetricsFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SiddhiContext {

    private static final Logger log = Logger.getLogger(SiddhiContext.class);

    private ExceptionHandler<Object> defaultDisrupterExceptionHandler;
    private Map<String, Class> siddhiExtensions;
    private PersistenceStore persistenceStore = null;
    private ConcurrentHashMap<String, DataSource> siddhiDataSources;
    private StatisticsConfiguration statisticsConfiguration;
    private ConcurrentHashMap<Class, AbstractExtensionHolder> extensionHolderMap;

    public SiddhiContext() {
        setSiddhiExtensions(SiddhiExtensionLoader.loadSiddhiExtensions());
        siddhiDataSources = new ConcurrentHashMap<String, DataSource>();
        statisticsConfiguration = new StatisticsConfiguration(new SiddhiMetricsFactory());
        extensionHolderMap = new ConcurrentHashMap<Class, AbstractExtensionHolder>();
        defaultDisrupterExceptionHandler = new ExceptionHandler<Object>() {
            @Override
            public void handleEventException(Throwable throwable, long l, Object event) {
                log.error("Disruptor encountered an error processing" +" [sequence: " + l + ", event: "+event.toString()+"]", throwable);
            }

            @Override
            public void handleOnStartException(Throwable throwable) {
                log.error("Disruptor encountered an error on start" , throwable);
            }

            @Override
            public void handleOnShutdownException(Throwable throwable) {
                log.error("Disruptor encountered an error on shutdown" , throwable);
            }
        };
    }

    public Map<String, Class> getSiddhiExtensions() {
        return siddhiExtensions;
    }

    public void setSiddhiExtensions(Map<String, Class> siddhiExtensions) {
        this.siddhiExtensions = siddhiExtensions;
    }

    public PersistenceStore getPersistenceStore() {
        return persistenceStore;
    }

    public void setPersistenceStore(PersistenceStore persistenceStore) {
        this.persistenceStore = persistenceStore;
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

}

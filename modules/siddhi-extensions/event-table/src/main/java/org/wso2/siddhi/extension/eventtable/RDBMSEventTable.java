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


package org.wso2.siddhi.extension.eventtable;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.exception.CannotLoadConfigurationException;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.OverwritingStreamEventExtractor;
import org.wso2.siddhi.core.util.collection.UpdateAttributeMapper;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.extension.eventtable.cache.CachingTable;
import org.wso2.siddhi.extension.eventtable.rdbms.*;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.definition.io.Store;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class RDBMSEventTable implements EventTable {

    private TableDefinition tableDefinition;
    private DBHandler dbHandler;
    private CachingTable cachedTable;
    private String cacheSizeInString;
    private boolean isCachingEnabled;
    private static final Logger log = Logger.getLogger(RDBMSEventTable.class);

    /*
    Loads rdbms-table-config.xml file which provides DB mapping details
     */
    static {
        try {
            DBQueryHelper.loadConfiguration();
        } catch (CannotLoadConfigurationException e) {
            throw new ExecutionPlanCreationException("Error while loading the rdbms configuration file", e);
        }
    }

    /**
     * Event Table initialization method, it checks the annotation and do necessary pre configuration tasks.
     *
     * @param tableDefinition        Definition of event table
     * @param tableMetaStreamEvent
     * @param tableStreamEventPool
     * @param tableStreamEventCloner
     * @param executionPlanContext   ExecutionPlan related meta information
     */
    public void init(TableDefinition tableDefinition, MetaStreamEvent tableMetaStreamEvent, StreamEventPool tableStreamEventPool, StreamEventCloner tableStreamEventCloner, ExecutionPlanContext executionPlanContext) {
        this.tableDefinition = tableDefinition;
        Connection con = null;
        int bloomFilterSize = RDBMSEventTableConstants.BLOOM_FILTER_SIZE;
        int bloomFilterHashFunctions = RDBMSEventTableConstants.BLOOM_FILTER_HASH_FUNCTIONS;
        String dataSourceName;
        String tableName;
        String cacheType;
        String cacheLoadingType;
        String cacheValidityInterval;
        String bloomsEnabled;
        String bloomFilterValidityInterval;


        Store store = tableDefinition.getStore();
        Map<String, String> getStoreOptions = null;
        if (store != null){
            getStoreOptions = store.getOptions();
        }

        Annotation fromAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_FROM,
                tableDefinition.getAnnotations()); //// TODO: 12/6/16 This must be deprecated

        if (getStoreOptions != null) {
            dataSourceName = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_DATASOURCE_NAME);
            tableName = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_TABLE_NAME);
        }
        else {
            dataSourceName = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_DATASOURCE_NAME);
            tableName = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_TABLE_NAME);
        }
        DataSource dataSource = executionPlanContext.getSiddhiContext().getSiddhiDataSource(dataSourceName);
        List<Attribute> attributeList = tableDefinition.getAttributeList();

        if (dataSource == null && dataSourceName == null) {
            String jdbcConnectionUrl;
            String username;
            String password;
            String driverName;

            if (getStoreOptions != null) {
                jdbcConnectionUrl = getStoreOptions.get(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_JDBC_URL);
                username = getStoreOptions.get(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_USERNAME);
                password = getStoreOptions.get(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_PASSWORD);
                driverName = getStoreOptions.get(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_DRIVER_NAME);
            } else {
                jdbcConnectionUrl = fromAnnotation.getElement(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_JDBC_URL);
                username = fromAnnotation.getElement(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_USERNAME);
                password = fromAnnotation.getElement(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_PASSWORD);
                driverName = fromAnnotation.getElement(RDBMSEventTableConstants.EVENT_TABLE_RDBMS_TABLE_DRIVER_NAME);
            }
            List<Element> connectionPropertyElements = null;

            Annotation connectionAnnotation = AnnotationHelper.getAnnotation(RDBMSEventTableConstants.ANNOTATION_CONNECTION, tableDefinition.getAnnotations());
            if (connectionAnnotation != null) {
                connectionPropertyElements = connectionAnnotation.getElements();
            }
            dataSource = PooledDataSource.getPoolDataSource(driverName, jdbcConnectionUrl, username, password, connectionPropertyElements);
        }

        if (dataSource == null) {
            throw new ExecutionPlanCreationException("Datasource specified for the event table is invalid/null");
        }
        if (tableName == null) {
            throw new ExecutionPlanCreationException("Invalid query specified. Required properties (tableName) not found ");
        }

        if (getStoreOptions != null) {
            cacheType = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE);
            cacheSizeInString = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_SIZE);
            cacheLoadingType = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_LOADING);
            cacheValidityInterval = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_VALIDITY_PERIOD);
            bloomsEnabled = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS);
            bloomFilterValidityInterval = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_VALIDITY_PERIOD);

        } else {
            cacheType = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE);
            cacheSizeInString = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_SIZE);
            cacheLoadingType = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_LOADING);
            cacheValidityInterval = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_CACHE_VALIDITY_PERIOD);
            bloomsEnabled = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS);
            bloomFilterValidityInterval = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_VALIDITY_PERIOD);
        }

        try {
            this.dbHandler = new DBHandler(dataSource, tableName, attributeList, tableDefinition);

            if ((con = dataSource.getConnection()) == null) {
                throw new ExecutionPlanCreationException("Error while making connection to database");
            }

            if (cacheType != null) {
                cachedTable = new CachingTable(cacheType, cacheSizeInString, executionPlanContext, tableDefinition);
                isCachingEnabled = true;

                if (cacheLoadingType != null && cacheLoadingType.equalsIgnoreCase(RDBMSEventTableConstants.EAGER_CACHE_LOADING_ELEMENT)) {
                    dbHandler.loadDBCache(cachedTable, cacheSizeInString);
                }

                if (cacheValidityInterval != null) {
                    Long cacheTimeInterval = Long.parseLong(cacheValidityInterval);
                    Timer timer = new Timer();
                    CacheUpdateTask cacheUpdateTask = new CacheUpdateTask();
                    timer.schedule(cacheUpdateTask, cacheTimeInterval);
                }

            } else if (bloomsEnabled != null && bloomsEnabled.equalsIgnoreCase("enable")) {
                String bloomsFilterSize;
                String bloomsFilterHash;
                if (getStoreOptions != null) {
                    bloomsFilterSize = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_SIZE);
                    bloomsFilterHash = getStoreOptions.get(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_HASH);
                } else {
                    bloomsFilterSize = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_SIZE);
                    bloomsFilterHash = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_HASH);
                }
                if (bloomsFilterSize != null) {
                    bloomFilterSize = Integer.parseInt(bloomsFilterSize);
                }
                if (bloomsFilterHash != null) {
                    bloomFilterHashFunctions = Integer.parseInt(bloomsFilterHash);
                }

                dbHandler.setBloomFilters(bloomFilterSize, bloomFilterHashFunctions);
                dbHandler.buildBloomFilters();
                if (bloomFilterValidityInterval != null) {
                    Long bloomTimeInterval = Long.parseLong(bloomFilterValidityInterval);
                    Timer timer = new Timer();
                    BloomsUpdateTask bloomsUpdateTask = new BloomsUpdateTask();
                    timer.schedule(bloomsUpdateTask, bloomTimeInterval, bloomTimeInterval);
                }
            }

        } catch (SQLException e) {
            throw new ExecutionPlanCreationException("Error while making connection to database", e);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    log.error("unable to release connection", e);
                }
            }
        }
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    /**
     * Called when adding an event to the event table
     *
     * @param addingEventChunk input event list
     */
    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        dbHandler.addEvent(addingEventChunk);
    }

    @Override
    public void update(ComplexEventChunk<StateEvent> updatingEventChunk, Operator operator, UpdateAttributeMapper[] updateAttributeMappers) {
        operator.update(updatingEventChunk, null, null);
        if (isCachingEnabled) {
            ((RDBMSOperator) operator).getInMemoryEventTableOperator().update(updatingEventChunk, cachedTable.getCacheList(), updateAttributeMappers);
        }
    }

    @Override
    public void overwriteOrAdd(ComplexEventChunk<StateEvent> overwritingOrAddingEventChunk, Operator operator, UpdateAttributeMapper[] updateAttributeMappers, OverwritingStreamEventExtractor overwritingStreamEventExtractor) {
        operator.overwriteOrAdd(overwritingOrAddingEventChunk, null, null, overwritingStreamEventExtractor);
        if (isCachingEnabled) {
            ((RDBMSOperator) operator).getInMemoryEventTableOperator().overwriteOrAdd(overwritingOrAddingEventChunk, cachedTable.getCacheList(), updateAttributeMappers, overwritingStreamEventExtractor);
        }
    }

    /**
     * Called when having "in" condition, to check the existence of the event
     *
     * @param matchingEvent Event that need to be check for existence
     * @param finder        Operator that perform RDBMS related search
     */
    @Override
    public synchronized boolean contains(StateEvent matchingEvent, Finder finder) {
        if (isCachingEnabled) {
            return ((RDBMSOperator) finder).getInMemoryEventTableOperator().contains(matchingEvent, cachedTable.getCacheList()) || finder.contains(matchingEvent, null);
        } else {
            return finder.contains(matchingEvent, null);
        }
    }

    /**
     * Called when deleting an event chunk from event table
     *
     * @param deletingEventChunk Event list for deletion
     * @param operator           Operator that perform RDBMS related operations
     */
    @Override
    public synchronized void delete(ComplexEventChunk deletingEventChunk, Operator operator) {
        operator.delete(deletingEventChunk, null);
        if (isCachingEnabled) {
            ((RDBMSOperator) operator).getInMemoryEventTableOperator().delete(deletingEventChunk, cachedTable.getCacheList());
        }
    }

    /**
     * Called to construct a operator to perform delete and update operations
     */
    @Override
    public Operator constructOperator(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return RDBMSOperatorParser.parse(dbHandler, expression, matchingMetaStateHolder, executionPlanContext, variableExpressionExecutors, eventTableMap, tableDefinition, cachedTable, tableDefinition.getId());
    }

    /**
     * Called to find a event from event table
     */
    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, null, null);
    }

    /**
     * Called to construct a operator to perform search operations
     */
    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return RDBMSOperatorParser.parse(dbHandler, expression, matchingMetaStateHolder, executionPlanContext, variableExpressionExecutors, eventTableMap, tableDefinition, cachedTable, tableDefinition.getId());
    }

    class CacheUpdateTask extends TimerTask {
        public void run() {
            cachedTable.invalidateCache();
            dbHandler.loadDBCache(cachedTable, cacheSizeInString);
        }
    }

    class BloomsUpdateTask extends TimerTask {
        public void run() {
            dbHandler.buildBloomFilters();
        }
    }

}

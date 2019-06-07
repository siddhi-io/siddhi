/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.util.cache;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.CacheTable;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.parser.MatcherParser;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.constant.LongConstant;
import io.siddhi.query.api.expression.math.Subtract;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.siddhi.core.util.SiddhiConstants.CACHE_EXPIRE_CURRENT_TIME;
import static io.siddhi.core.util.SiddhiConstants.CACHE_TABLE_TIMESTAMP_ADDED;
import static io.siddhi.core.util.cache.CacheUtils.addRequiredFieldsToDataForCache;
import static io.siddhi.core.util.cache.CacheUtils.findEventChunkSize;

/**
 * this class is a runnable which runs on a separate thread called by AbstractQueryableRecordTable and handles cache
 * expiry
 */
public class CacheExpiryHandler {
    private static final Logger log = Logger.getLogger(CacheExpiryHandler.class);
    private InMemoryTable cacheTable;
    private StreamEventFactory streamEventFactory;
    private Map<String, Table> tableMap;
    private AbstractQueryableRecordTable storeTable;
    private SiddhiAppContext siddhiAppContext;
    private long expiryTime;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private CompiledCondition cacheExpiryCompiledCondition;

    public CacheExpiryHandler(long expiryTime, InMemoryTable cacheTable, Map<String, Table> tableMap,
                              AbstractQueryableRecordTable storeTable, SiddhiAppContext siddhiAppContext) {
        this.cacheTable = cacheTable;
        this.tableMap = tableMap;
        this.storeTable = storeTable;
        this.siddhiAppContext = siddhiAppContext;
        this.expiryTime = expiryTime;

        cacheExpiryCompiledCondition = generateExpiryCompiledCondition();
    }

    private ComplexEventChunk<StateEvent> generateDeleteEventChunk() {
        ComplexEventChunk<StateEvent> deleteEventChunk = new ComplexEventChunk<>();
        StateEvent stateEvent = new StateEvent(2, 0);
        StreamEvent deletingEvent = streamEventFactory.newInstance();
        deletingEvent.setOutputData(new Object[]{siddhiAppContext.getTimestampGenerator().currentTime()});
        stateEvent.addEvent(0, deletingEvent);
        deleteEventChunk.add(stateEvent);
        return deleteEventChunk;
    }

    private CompiledCondition generateExpiryCompiledCondition() {
        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        TableDefinition matchingTableDefinition = TableDefinition.id(cacheTable.getTableDefinition().getId());
        for (Attribute attribute : cacheTable.getTableDefinition().getAttributeList()) {
            tableMetaStreamEvent.addOutputData(attribute);
            matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);
        streamEventFactory = new StreamEventFactory(tableMetaStreamEvent);

        Variable rightExpressionForSubtract = new Variable(CACHE_TABLE_TIMESTAMP_ADDED);
        rightExpressionForSubtract.setStreamId(cacheTable.getTableDefinition().getId());
        Expression rightExpressionForCompare = new LongConstant(expiryTime);
        Compare.Operator greaterThanOperator = Compare.Operator.GREATER_THAN;

        MetaStreamEvent currentTimeMetaStreamEvent = new MetaStreamEvent();
        currentTimeMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        Attribute currentTimeAttribute = new Attribute(CACHE_EXPIRE_CURRENT_TIME, Attribute.Type.LONG);
        currentTimeMetaStreamEvent.addOutputData(currentTimeAttribute);
        TableDefinition currentTimeTableDefinition = TableDefinition.id("");
        currentTimeTableDefinition.attribute(CACHE_EXPIRE_CURRENT_TIME, Attribute.Type.LONG);
        currentTimeMetaStreamEvent.addInputDefinition(currentTimeTableDefinition);

        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        metaStateEvent.addEvent(currentTimeMetaStreamEvent);
        metaStateEvent.addEvent(tableMetaStreamEvent);

        MatchingMetaInfoHolder matchingMetaInfoHolder =
                MatcherParser.constructMatchingMetaStateHolder(metaStateEvent, 0,
                        cacheTable.getTableDefinition(), 0);
        List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
        Expression leftExpressionForSubtract = new Variable(CACHE_EXPIRE_CURRENT_TIME);
        Expression leftExpressionForCompare = new Subtract(leftExpressionForSubtract, rightExpressionForSubtract);
        Expression deleteCondition = new Compare(leftExpressionForCompare, greaterThanOperator,
                rightExpressionForCompare);
        SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, "expiryDeleteQuery");
        return cacheTable.compileCondition(deleteCondition, matchingMetaInfoHolder, variableExpressionExecutors,
                tableMap, siddhiQueryContext);
    }

    private void handleCacheExpiry() {
        StateEvent stateEventForCaching = new StateEvent(1, 0);
        StreamEvent loadedDataFromStore = null;

        if (storeTable.getStoreTableSize() != -1 && storeTable.getStoreSizeLastCheckedTime() > //todo: remove store table load size check in find and query
                        siddhiAppContext.getTimestampGenerator().currentTime() - 30000) { //todo: use a multiple of cache expiry param
            //todo: if store table sioze becomes smaller than cache reload them into cache
            log.info(siddhiAppContext.getName() + ": store table size is new");
            if (storeTable.getStoreTableSize() <= storeTable.getMaxCacheSize()) {
                try {
                    loadedDataFromStore = storeTable.queryFromStore(stateEventForCaching,
                            storeTable.getCompiledConditionForCaching(),
                            storeTable.getCompiledSelectionForCaching(), storeTable.getOutputAttributesForCaching());
                } catch (ConnectionUnavailableException ignored) {

                }
                deleteAndReloadExpiredEvents(loadedDataFromStore);
            } else {
                CompiledCondition cc = cacheExpiryCompiledCondition;
                cacheTable.delete(generateDeleteEventChunk(),
                        cc);
            }
        } else {
            log.info(siddhiAppContext.getName() + ": store table size is old"); //todo: change the message meaningful
            try {
                loadedDataFromStore = storeTable.queryFromStore(stateEventForCaching,
                        storeTable.getCompiledConditionForCaching(),
                        storeTable.getCompiledSelectionForCaching(), storeTable.getOutputAttributesForCaching());
            } catch (ConnectionUnavailableException ignored) {
            //todo: log a warining saying that not able to connect. move
            } //todo": if cache is greater than store size delete everythong efficiently without going through
            storeTable.setStoreTableSize(findEventChunkSize(loadedDataFromStore)); //todo: move everything inside try catch
            storeTable.setStoreSizeLastCheckedTime(siddhiAppContext.getTimestampGenerator().currentTime());
            if (storeTable.getStoreTableSize() <= storeTable.getMaxCacheSize()) {
                deleteAndReloadExpiredEvents(loadedDataFromStore);
            } else {
                CompiledCondition cc = cacheExpiryCompiledCondition;
                cacheTable.delete(generateDeleteEventChunk(), cc);
            }
        }
    }

    private void deleteAndReloadExpiredEvents(StreamEvent loadedDataFromStore) {
        ComplexEventChunk<StreamEvent> addingEventChunkWithTimestamp = new ComplexEventChunk<>(true);

        while (loadedDataFromStore != null) {
            addRequiredFieldsToDataForCache(addingEventChunkWithTimestamp,
                    loadedDataFromStore, siddhiAppContext, storeTable.getCachePolicy(), true);
            if (loadedDataFromStore.getNext() == null) {
                break;
            }
            loadedDataFromStore = loadedDataFromStore.getNext();
        }
        readWriteLock.writeLock().lock();
        try {
            ((CacheTable) cacheTable).deleteAll();
            if (loadedDataFromStore != null) {
                cacheTable.add(addingEventChunkWithTimestamp);
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

//    public void simpleExpire() {
//        CompiledCondition cc = generateExpiryCompiledCondition(
//                siddhiAppContext.getTimestampGenerator().currentTime());
//        cacheTable.delete(generateDeleteEventChunk(), cc);
//    }

    public Runnable checkAndExpireCache() {
        class CheckAndExpireCache implements Runnable {

            public CheckAndExpireCache() {
            }

            @Override
            public void run() {
                try {
                    handleCacheExpiry();
                } catch (Exception e) {
                    throw new SiddhiAppRuntimeException(siddhiAppContext.getName() + ": " + e.getMessage());
                }
//                simpleExpire();
            }
        }
        return new CheckAndExpireCache();
    }
}
